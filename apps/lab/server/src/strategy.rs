//! The strategy language an agent submits, and the engine strategy that runs it.
//!
//! A strategy is a few lines of text plus JSON parameters. The worker parses
//! it here and runs it inside the h5i-db backtest engine, on the agent's fork,
//! through the service handle. Nothing in the language opens a file, a socket,
//! or a process: the only data it sees is what the replay delivers, so the
//! decision-time ceiling and the sealed holdout hold for it without a sandbox.
//!
//! ```text
//! # 10/40 moving-average crossover
//! universe ALFA BRVO
//! let fast = sma($fast)
//! let slow = sma($slow)
//! long when fast > slow
//! flat when fast < slow
//! size $units
//! ```
//!
//! On every book snapshot of an instrument in `universe`, the program reads
//! that instrument's mid-price history. When `long when` holds, the target
//! position is `size` units. Otherwise, when `flat when` holds, it is zero.
//! Otherwise the position is kept. A market order closes the gap between the
//! position and the target. Values that need more history than has arrived
//! are not numbers, and a comparison with one is false, so nothing trades
//! during warm-up.
//!
//! Expressions: numbers, `$name` (a number from the parameters), `let`
//! names, `mid`, `spread`, `position`, `+ - * /`, `< <= > >= == !=`, `and`,
//! `or`, `not`, parentheses, and these functions of the mid-price, each
//! taking a lookback in snapshots: `sma(n)`, `ema(n)`, `std(n)`,
//! `zscore(n)`, `ret(n)`, `high(n)`, `low(n)`.

use std::collections::{BTreeMap, VecDeque};

use h5i_db_backtest::engine::{Context, OrderRequest, Strategy};
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{InstrumentId, OutcomeId};
use h5i_db_backtest::order::Fill;
use h5i_db_backtest::types::{Qty, Side};

/// Longest lookback a function may ask for, in snapshots.
pub const MAX_LOOKBACK: usize = 10_000;
/// Longest program, in bytes.
pub const MAX_CODE: usize = 8_192;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Func {
    Sma,
    Ema,
    Std,
    Zscore,
    Ret,
    High,
    Low,
}

impl Func {
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "sma" => Func::Sma,
            "ema" => Func::Ema,
            "std" => Func::Std,
            "zscore" => Func::Zscore,
            "ret" => Func::Ret,
            "high" => Func::High,
            "low" => Func::Low,
            _ => return None,
        })
    }

    /// Mid-prices this function needs before it is a number.
    fn history(self, n: usize) -> usize {
        match self {
            Func::Ret => n + 1,
            Func::Ema => n * 3,
            _ => n,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Op {
    Add,
    Sub,
    Mul,
    Div,
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
    And,
    Or,
}

#[derive(Clone, Debug, PartialEq)]
enum Expr {
    Num(f64),
    Mid,
    Spread,
    Position,
    Var(usize),
    Call(Func, usize),
    Neg(Box<Expr>),
    Not(Box<Expr>),
    Bin(Op, Box<Expr>, Box<Expr>),
}

/// A parsed strategy with its parameters bound.
#[derive(Clone, Debug)]
pub struct Program {
    universe: Vec<InstrumentId>,
    lets: Vec<Expr>,
    long: Expr,
    flat: Option<Expr>,
    size: Expr,
    history: usize,
}

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Num(f64),
    Ident(String),
    Param(String),
    Sym(&'static str),
}

fn tokenize(text: &str) -> Result<Vec<Tok>, String> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c.is_ascii_digit()
            || (c == '.' && chars.get(i + 1).is_some_and(|d| d.is_ascii_digit()))
        {
            let start = i;
            while i < chars.len()
                && (chars[i].is_ascii_digit() || chars[i] == '.' || chars[i] == '_')
            {
                i += 1;
            }
            let text: String = chars[start..i].iter().filter(|c| **c != '_').collect();
            out.push(Tok::Num(
                text.parse().map_err(|_| format!("bad number {text:?}"))?,
            ));
        } else if c.is_ascii_alphabetic() || c == '_' || c == '$' {
            let start = i;
            i += 1;
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            if let Some(name) = word.strip_prefix('$') {
                if name.is_empty() {
                    return Err("`$` needs a parameter name".into());
                }
                out.push(Tok::Param(name.to_string()));
            } else {
                out.push(Tok::Ident(word));
            }
        } else {
            let two: String = chars[i..chars.len().min(i + 2)].iter().collect();
            let sym = match two.as_str() {
                "<=" => Some("<="),
                ">=" => Some(">="),
                "==" => Some("=="),
                "!=" => Some("!="),
                _ => None,
            };
            if let Some(sym) = sym {
                out.push(Tok::Sym(sym));
                i += 2;
                continue;
            }
            let sym = match c {
                '+' => "+",
                '-' => "-",
                '*' => "*",
                '/' => "/",
                '<' => "<",
                '>' => ">",
                '(' => "(",
                ')' => ")",
                '=' => "=",
                _ => return Err(format!("unexpected character {c:?}")),
            };
            out.push(Tok::Sym(sym));
            i += 1;
        }
    }
    Ok(out)
}

struct Parser<'a> {
    toks: Vec<Tok>,
    at: usize,
    params: &'a serde_json::Map<String, serde_json::Value>,
    names: &'a [String],
    history: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.at)
    }

    fn next(&mut self) -> Option<Tok> {
        let tok = self.toks.get(self.at).cloned();
        self.at += 1;
        tok
    }

    fn eat_sym(&mut self, sym: &str) -> bool {
        if matches!(self.peek(), Some(Tok::Sym(s)) if *s == sym) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn eat_word(&mut self, word: &str) -> bool {
        if matches!(self.peek(), Some(Tok::Ident(w)) if w == word) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn param(&self, name: &str) -> Result<f64, String> {
        match self.params.get(name) {
            Some(value) => value
                .as_f64()
                .filter(|v| v.is_finite())
                .ok_or_else(|| format!("parameter ${name} is not a number")),
            None => Err(format!("parameter ${name} is not set")),
        }
    }

    fn expr(&mut self) -> Result<Expr, String> {
        let mut left = self.and()?;
        while self.eat_word("or") {
            let right = self.and()?;
            left = Expr::Bin(Op::Or, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<Expr, String> {
        let mut left = self.not()?;
        while self.eat_word("and") {
            let right = self.not()?;
            left = Expr::Bin(Op::And, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn not(&mut self) -> Result<Expr, String> {
        if self.eat_word("not") {
            return Ok(Expr::Not(Box::new(self.not()?)));
        }
        self.cmp()
    }

    fn cmp(&mut self) -> Result<Expr, String> {
        let left = self.add()?;
        let op = match self.peek() {
            Some(Tok::Sym("<")) => Op::Lt,
            Some(Tok::Sym("<=")) => Op::Le,
            Some(Tok::Sym(">")) => Op::Gt,
            Some(Tok::Sym(">=")) => Op::Ge,
            Some(Tok::Sym("==")) => Op::Eq,
            Some(Tok::Sym("!=")) => Op::Ne,
            _ => return Ok(left),
        };
        self.at += 1;
        let right = self.add()?;
        Ok(Expr::Bin(op, Box::new(left), Box::new(right)))
    }

    fn add(&mut self) -> Result<Expr, String> {
        let mut left = self.mul()?;
        loop {
            let op = if self.eat_sym("+") {
                Op::Add
            } else if self.eat_sym("-") {
                Op::Sub
            } else {
                return Ok(left);
            };
            let right = self.mul()?;
            left = Expr::Bin(op, Box::new(left), Box::new(right));
        }
    }

    fn mul(&mut self) -> Result<Expr, String> {
        let mut left = self.unary()?;
        loop {
            let op = if self.eat_sym("*") {
                Op::Mul
            } else if self.eat_sym("/") {
                Op::Div
            } else {
                return Ok(left);
            };
            let right = self.unary()?;
            left = Expr::Bin(op, Box::new(left), Box::new(right));
        }
    }

    fn unary(&mut self) -> Result<Expr, String> {
        if self.eat_sym("-") {
            return Ok(Expr::Neg(Box::new(self.unary()?)));
        }
        self.atom()
    }

    /// A lookback: a positive whole number, written out or as a parameter.
    fn lookback(&mut self) -> Result<usize, String> {
        let value = match self.next() {
            Some(Tok::Num(n)) => n,
            Some(Tok::Param(name)) => self.param(&name)?,
            _ => return Err("a lookback is a number or a $parameter".into()),
        };
        if value.fract() != 0.0 || value < 1.0 || value > MAX_LOOKBACK as f64 {
            return Err(format!(
                "lookback {value} is not a whole number in 1..={MAX_LOOKBACK}"
            ));
        }
        Ok(value as usize)
    }

    fn atom(&mut self) -> Result<Expr, String> {
        match self.next() {
            Some(Tok::Num(n)) => Ok(Expr::Num(n)),
            Some(Tok::Param(name)) => Ok(Expr::Num(self.param(&name)?)),
            Some(Tok::Sym("(")) => {
                let inner = self.expr()?;
                if !self.eat_sym(")") {
                    return Err("missing `)`".into());
                }
                Ok(inner)
            }
            Some(Tok::Ident(word)) => {
                if let Some(func) = Func::parse(&word) {
                    if !self.eat_sym("(") {
                        return Err(format!("{word} takes a lookback: {word}(n)"));
                    }
                    let n = self.lookback()?;
                    if !self.eat_sym(")") {
                        return Err(format!("missing `)` after {word}"));
                    }
                    self.history = self.history.max(func.history(n));
                    return Ok(Expr::Call(func, n));
                }
                match word.as_str() {
                    "mid" => Ok(Expr::Mid),
                    "spread" => Ok(Expr::Spread),
                    "position" => Ok(Expr::Position),
                    _ => match self.names.iter().position(|name| *name == word) {
                        Some(index) => Ok(Expr::Var(index)),
                        None => Err(format!("unknown name {word:?}")),
                    },
                }
            }
            Some(tok) => Err(format!("unexpected {tok:?}")),
            None => Err("the line ends early".into()),
        }
    }
}

fn reserved(word: &str) -> bool {
    Func::parse(word).is_some()
        || matches!(
            word,
            "mid"
                | "spread"
                | "position"
                | "and"
                | "or"
                | "not"
                | "let"
                | "long"
                | "flat"
                | "size"
                | "universe"
                | "when"
        )
}

impl Program {
    /// Parse `code` with `params`, a JSON object of numbers.
    pub fn compile(code: &str, params: &serde_json::Value) -> Result<Program, String> {
        if code.len() > MAX_CODE {
            return Err(format!("the program is longer than {MAX_CODE} bytes"));
        }
        let empty = serde_json::Map::new();
        let params = match params {
            serde_json::Value::Object(map) => map,
            serde_json::Value::Null => &empty,
            _ => return Err("parameters are a JSON object".into()),
        };
        let mut universe: Vec<InstrumentId> = Vec::new();
        let mut names: Vec<String> = Vec::new();
        let mut lets = Vec::new();
        let mut long = None;
        let mut flat = None;
        let mut size = None;
        let mut history = 1;
        for (number, raw) in code.lines().enumerate() {
            let line = raw.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let at = |error: String| format!("line {}: {error}", number + 1);
            let toks = tokenize(line).map_err(at)?;
            let mut parser = Parser {
                toks,
                at: 0,
                params,
                names: &names,
                history,
            };
            let parsed = if parser.eat_word("universe") {
                let mut ids = Vec::new();
                while let Some(Tok::Ident(id)) = parser.next() {
                    ids.push(InstrumentId::new(id.as_str()).map_err(|e| at(e.to_string()))?);
                }
                if ids.is_empty() || parser.at != parser.toks.len() + 1 {
                    return Err(at("universe lists instrument ids".into()));
                }
                universe.extend(ids);
                continue;
            } else if parser.eat_word("let") {
                let Some(Tok::Ident(name)) = parser.next() else {
                    return Err(at("let needs a name".into()));
                };
                if reserved(&name) || names.contains(&name) {
                    return Err(at(format!("{name:?} is already a name")));
                }
                if !parser.eat_sym("=") {
                    return Err(at("let name = expression".into()));
                }
                let expr = parser.expr().map_err(at)?;
                (Some(name), expr, 0)
            } else if parser.eat_word("long") {
                if !parser.eat_word("when") {
                    return Err(at("long when <condition>".into()));
                }
                (None, parser.expr().map_err(at)?, 1)
            } else if parser.eat_word("flat") {
                if !parser.eat_word("when") {
                    return Err(at("flat when <condition>".into()));
                }
                (None, parser.expr().map_err(at)?, 2)
            } else if parser.eat_word("size") {
                (None, parser.expr().map_err(at)?, 3)
            } else if parser.eat_word("short") {
                return Err(at(
                    "short positions are not supported on spot instruments".into()
                ));
            } else {
                return Err(at(
                    "a line is universe, let, long when, flat when, or size".into()
                ));
            };
            if parser.at != parser.toks.len() {
                return Err(at(format!("unexpected {:?}", parser.toks[parser.at])));
            }
            history = parser.history;
            let (name, expr, slot) = parsed;
            let slot_name = ["let", "long", "flat", "size"][slot];
            let duplicate = match slot {
                0 => false,
                1 => long.is_some(),
                2 => flat.is_some(),
                _ => size.is_some(),
            };
            if duplicate {
                return Err(at(format!("a second `{slot_name}` line")));
            }
            match slot {
                0 => {
                    names.push(name.expect("let has a name"));
                    lets.push(expr);
                }
                1 => long = Some(expr),
                2 => flat = Some(expr),
                _ => size = Some(expr),
            }
        }
        if universe.is_empty() {
            return Err("the program needs a universe line".into());
        }
        let long = long.ok_or("the program needs a `long when` line")?;
        Ok(Program {
            universe,
            lets,
            long,
            flat,
            size: size.unwrap_or(Expr::Num(1.0)),
            history,
        })
    }
}

/// Parameters in one canonical text: an object with sorted keys, no spaces.
pub fn canonical_params(params: &serde_json::Value) -> Result<String, String> {
    match params {
        serde_json::Value::Object(map) => {
            let sorted: BTreeMap<&String, &serde_json::Value> = map.iter().collect();
            serde_json::to_string(&sorted).map_err(|e| e.to_string())
        }
        serde_json::Value::Null => Ok("{}".into()),
        _ => Err("parameters are a JSON object".into()),
    }
}

/// What a run's fills add up to, seen from the strategy's `on_fill`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Tally {
    pub fills: u64,
    pub orders: u64,
    /// Commissions plus the half-spread paid against the mid when crossing.
    pub costs: f64,
    pub commissions: f64,
    /// Round trips from flat back to flat, and how many made money.
    pub round_trips: u64,
    pub wins: u64,
}

#[derive(Clone, Debug, Default)]
struct Book {
    mids: VecDeque<f64>,
    spread: f64,
    /// Average entry price and the round trip's running pnl, for win rate.
    held: f64,
    entry: f64,
    trip_pnl: f64,
}

/// The engine strategy that runs a [`Program`].
pub struct RuleStrategy {
    program: Program,
    books: BTreeMap<InstrumentId, Book>,
    pub tally: Tally,
    /// The account's equity as the engine holds it, at each book snapshot:
    /// cash plus every position at the engine's mark. One point per instant.
    pub curve: Vec<(i64, f64)>,
}

const LOT: f64 = 0.001;

impl RuleStrategy {
    pub fn new(program: Program) -> Self {
        Self {
            program,
            books: BTreeMap::new(),
            tally: Tally::default(),
            curve: Vec::new(),
        }
    }

    fn sample(&mut self, ctx: &Context<'_>, at: i64) {
        let mut equity = ctx.cash().to_f64();
        for position in ctx.portfolio().positions() {
            let quantity = position.quantity.to_f64();
            if quantity == 0.0 {
                continue;
            }
            let mark = ctx
                .mark(&position.instrument, position.outcome)
                .map(|price| price.to_f64())
                .unwrap_or(position.average_price.to_f64());
            equity += quantity * mark;
        }
        match self.curve.last_mut() {
            Some(last) if last.0 == at => last.1 = equity,
            _ => self.curve.push((at, equity)),
        }
    }
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn deviation(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return f64::NAN;
    }
    let m = mean(values);
    (values.iter().map(|v| (v - m).powi(2)).sum::<f64>() / (values.len() - 1) as f64).sqrt()
}

fn truth(value: bool) -> f64 {
    if value { 1.0 } else { 0.0 }
}

struct Env<'a> {
    mids: &'a [f64],
    spread: f64,
    position: f64,
    vars: &'a [f64],
}

impl Env<'_> {
    fn last(&self, n: usize) -> Option<&[f64]> {
        (self.mids.len() >= n).then(|| &self.mids[self.mids.len() - n..])
    }

    fn call(&self, func: Func, n: usize) -> f64 {
        let Some(window) = self.last(func.history(n)) else {
            return f64::NAN;
        };
        let mid = *self.mids.last().expect("history is not empty");
        match func {
            Func::Sma => mean(window),
            Func::Std => deviation(window),
            Func::Zscore => {
                let sd = deviation(window);
                if sd > 0.0 {
                    (mid - mean(window)) / sd
                } else {
                    f64::NAN
                }
            }
            Func::Ret => mid / window[0] - 1.0,
            Func::High => window.iter().copied().fold(f64::NEG_INFINITY, f64::max),
            Func::Low => window.iter().copied().fold(f64::INFINITY, f64::min),
            Func::Ema => {
                let alpha = 2.0 / (n as f64 + 1.0);
                window[1..]
                    .iter()
                    .fold(window[0], |ema, value| ema + alpha * (value - ema))
            }
        }
    }

    fn eval(&self, expr: &Expr) -> f64 {
        match expr {
            Expr::Num(n) => *n,
            Expr::Mid => self.mids.last().copied().unwrap_or(f64::NAN),
            Expr::Spread => self.spread,
            Expr::Position => self.position,
            Expr::Var(index) => self.vars[*index],
            Expr::Call(func, n) => self.call(*func, *n),
            Expr::Neg(inner) => -self.eval(inner),
            Expr::Not(inner) => {
                let value = self.eval(inner);
                if value.is_nan() {
                    f64::NAN
                } else {
                    truth(value == 0.0)
                }
            }
            Expr::Bin(op, left, right) => {
                let a = self.eval(left);
                let b = self.eval(right);
                if a.is_nan() || b.is_nan() {
                    return f64::NAN;
                }
                match op {
                    Op::Add => a + b,
                    Op::Sub => a - b,
                    Op::Mul => a * b,
                    Op::Div => a / b,
                    Op::Lt => truth(a < b),
                    Op::Le => truth(a <= b),
                    Op::Gt => truth(a > b),
                    Op::Ge => truth(a >= b),
                    Op::Eq => truth(a == b),
                    Op::Ne => truth(a != b),
                    Op::And => truth(a != 0.0 && b != 0.0),
                    Op::Or => truth(a != 0.0 || b != 0.0),
                }
            }
        }
    }
}

/// True only for a number other than zero. Not a number is false.
fn holds(value: f64) -> bool {
    value.is_finite() && value != 0.0
}

fn top(levels: &[(h5i_db_backtest::types::Price, Qty)], best: fn(f64, f64) -> f64) -> Option<f64> {
    levels
        .iter()
        .filter(|(_, size)| size.is_positive())
        .map(|(price, _)| price.to_f64())
        .reduce(best)
}

impl Strategy for RuleStrategy {
    fn on_event(&mut self, ctx: &mut Context<'_>, record: &Record) -> h5i_db_backtest::Result<()> {
        let MarketEvent::BookSnapshot { bids, asks } = &record.event else {
            return Ok(());
        };
        self.sample(ctx, record.ts().get());
        if !self.program.universe.contains(&record.instrument) {
            return Ok(());
        }
        let (Some(bid), Some(ask)) = (top(bids, f64::max), top(asks, f64::min)) else {
            return Ok(());
        };
        let keep = self.program.history.max(1);
        let book = self.books.entry(record.instrument.clone()).or_default();
        book.mids.push_back((bid + ask) / 2.0);
        while book.mids.len() > keep {
            book.mids.pop_front();
        }
        book.spread = ask - bid;
        let position = ctx
            .position_quantity(&record.instrument, OutcomeId::FIRST)
            .to_f64();
        let mids: Vec<f64> = book.mids.iter().copied().collect();
        let mut vars = Vec::with_capacity(self.program.lets.len());
        for expr in &self.program.lets {
            let env = Env {
                mids: &mids,
                spread: book.spread,
                position,
                vars: &vars,
            };
            let value = env.eval(expr);
            vars.push(value);
        }
        let env = Env {
            mids: &mids,
            spread: book.spread,
            position,
            vars: &vars,
        };
        let target = if holds(env.eval(&self.program.long)) {
            env.eval(&self.program.size)
        } else if self
            .program
            .flat
            .as_ref()
            .is_some_and(|flat| holds(env.eval(flat)))
        {
            0.0
        } else {
            return Ok(());
        };
        if !target.is_finite() || target < 0.0 {
            return Ok(());
        }
        let gap = ((target - position) / LOT).round() * LOT;
        if gap.abs() < LOT {
            return Ok(());
        }
        let (side, quantity) = if gap > 0.0 {
            (Side::Buy, gap)
        } else {
            (Side::Sell, -gap)
        };
        let Ok(quantity) = Qty::from_f64(quantity) else {
            return Ok(());
        };
        self.tally.orders += 1;
        ctx.submit(OrderRequest::market(
            record.instrument.clone(),
            OutcomeId::FIRST,
            side,
            quantity,
        ));
        Ok(())
    }

    fn on_fill(&mut self, _ctx: &mut Context<'_>, fill: &Fill) -> h5i_db_backtest::Result<()> {
        let price = fill.price.to_f64();
        let quantity = fill.quantity.to_f64().abs();
        let commission = fill.commission.to_f64();
        self.tally.fills += 1;
        self.tally.commissions += commission;
        self.tally.costs += commission;
        let book = self.books.entry(fill.instrument.clone()).or_default();
        if let Some(mid) = book.mids.back() {
            self.tally.costs += (price - mid).abs() * quantity;
        }
        match fill.side {
            Side::Buy => {
                let held = book.held + quantity;
                if held > 0.0 {
                    book.entry = (book.entry * book.held + price * quantity) / held;
                }
                book.held = held;
                book.trip_pnl -= commission;
            }
            Side::Sell => {
                let closed = quantity.min(book.held);
                book.trip_pnl += (price - book.entry) * closed - commission;
                book.held -= closed;
                if book.held < LOT / 2.0 {
                    book.held = 0.0;
                    self.tally.round_trips += 1;
                    if book.trip_pnl > 0.0 {
                        self.tally.wins += 1;
                    }
                    book.trip_pnl = 0.0;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CROSS: &str = "# crossover\nuniverse ALFA BRVO\nlet fast = sma($fast)\nlet slow = sma($slow)\nlong when fast > slow\nflat when fast < slow\nsize $units\n";

    #[test]
    fn a_crossover_compiles_with_its_parameters() {
        let params = serde_json::json!({"fast": 5, "slow": 20, "units": 10});
        let program = Program::compile(CROSS, &params).unwrap();
        assert_eq!(program.universe.len(), 2);
        assert_eq!(program.history, 20);
        assert_eq!(program.size, Expr::Num(10.0));
    }

    #[test]
    fn mistakes_are_named_with_their_line() {
        let params = serde_json::json!({"fast": 5});
        let error = Program::compile(CROSS, &params).unwrap_err();
        assert!(
            error.contains("line 4") && error.contains("$slow"),
            "{error}"
        );
        let error = Program::compile("universe A\nlong when sma(0) > 1", &params).unwrap_err();
        assert!(error.contains("lookback"), "{error}");
        let error = Program::compile("universe A\nlong when foo > 1", &params).unwrap_err();
        assert!(error.contains("unknown name"), "{error}");
        assert!(Program::compile("long when mid > 1", &params).is_err());
        assert!(Program::compile("universe A\nflat when mid > 1", &params).is_err());
        assert!(
            Program::compile("universe A\nlong when mid > 1\nlong when mid < 1", &params).is_err()
        );
        assert!(Program::compile("universe A\nshort when mid > 1", &params).is_err());
        assert!(
            Program::compile("universe A\nlong when mid > 1", &serde_json::json!([1])).is_err()
        );
    }

    #[test]
    fn functions_wait_for_their_history() {
        let mids = [10.0, 11.0, 12.0, 13.0];
        let env = Env {
            mids: &mids,
            spread: 0.1,
            position: 0.0,
            vars: &[],
        };
        assert_eq!(env.call(Func::Sma, 2), 12.5);
        assert!(env.call(Func::Sma, 5).is_nan());
        assert!((env.call(Func::Ret, 3) - 0.3).abs() < 1e-12);
        assert!(env.call(Func::Ret, 4).is_nan());
        assert_eq!(env.call(Func::High, 3), 13.0);
        assert_eq!(env.call(Func::Low, 3), 11.0);
        let gt = Expr::Bin(
            Op::Gt,
            Box::new(Expr::Call(Func::Sma, 9)),
            Box::new(Expr::Num(0.0)),
        );
        assert!(!holds(env.eval(&gt)));
    }

    #[test]
    fn parameters_have_one_canonical_text() {
        let a = canonical_params(&serde_json::json!({"slow": 20, "fast": 5})).unwrap();
        let b = canonical_params(&serde_json::json!({"fast": 5, "slow": 20})).unwrap();
        assert_eq!(a, b);
        assert_eq!(a, "{\"fast\":5,\"slow\":20}");
    }
}
