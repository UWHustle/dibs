use std::fmt;
use std::fmt::Write;

#[derive(Clone, Copy)]
pub enum Comparison {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

impl fmt::Display for Comparison {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_char(match self {
            Comparison::Equal => '=',
            Comparison::NotEqual => '≠',
            Comparison::Less => '<',
            Comparison::LessEqual => '≤',
            Comparison::Greater => '>',
            Comparison::GreaterEqual => '≥',
        })
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum Operand {
    Column(usize),
    Variable(usize),
}

impl fmt::Display for Operand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Operand::Column(id) => write!(f, "col({})", id),
            Operand::Variable(id) => write!(f, "var({})", id),
        }
    }
}

#[derive(Clone, PartialEq)]
pub enum Connective {
    Conjunction,
    Disjunction,
}

#[derive(Clone)]
pub enum Predicate {
    Comparison(Comparison, Operand, Operand),
    Connective(Connective, Vec<Predicate>),
}

impl Predicate {
    pub fn comparison(comparison: Comparison, left: Operand, right: Operand) -> Self {
        Predicate::Comparison(comparison, left, right)
    }

    pub fn conjunction(terms: Vec<Predicate>) -> Self {
        Predicate::Connective(Connective::Conjunction, terms)
    }

    pub fn disjunction(terms: Vec<Predicate>) -> Self {
        Predicate::Connective(Connective::Disjunction, terms)
    }

    pub fn boolean(v: bool) -> Self {
        if v {
            Predicate::conjunction(Default::default())
        } else {
            Predicate::disjunction(Default::default())
        }
    }

    pub fn condense(self) -> Self {
        match self {
            Predicate::Connective(connective, mut terms) => {
                let mut new_terms = vec![];

                while let Some(term) = terms.pop() {
                    let new_term = term.condense();
                    match new_term {
                        Predicate::Connective(sub_connective, sub_terms) => {
                            if connective == sub_connective {
                                new_terms.extend(sub_terms);
                            } else {
                                if sub_terms.is_empty() {
                                    return Predicate::Connective(sub_connective, sub_terms);
                                }

                                new_terms.push(Predicate::Connective(sub_connective, sub_terms));
                            }
                        }
                        comparison => new_terms.push(comparison),
                    };
                }

                if new_terms.len() == 1 {
                    new_terms.pop().unwrap()
                } else {
                    Predicate::Connective(connective, new_terms)
                }
            }
            _ => self,
        }
    }

    fn fmt_internal(
        &self,
        f: &mut fmt::Formatter,
        mut indent: String,
        first: bool,
        last: bool,
    ) -> fmt::Result {
        f.write_str(&indent)?;

        if !first && last {
            f.write_str("└── ")?;
            indent += "    ";
        } else if !last {
            f.write_str("├── ")?;
            indent += "│   ";
        }

        match self {
            Predicate::Comparison(comparison, left, right) => {
                write!(f, "{} {} {}", left, comparison, right)?;
            }
            Predicate::Connective(connective, terms) => {
                if terms.is_empty() {
                    match connective {
                        Connective::Conjunction => f.write_str("TRUE")?,
                        Connective::Disjunction => f.write_str("FALSE")?,
                    }
                } else {
                    match connective {
                        Connective::Conjunction => f.write_str("AND")?,
                        Connective::Disjunction => f.write_str("OR")?,
                    }

                    for i in 0..terms.len() {
                        f.write_char('\n')?;
                        terms[i].fmt_internal(f, indent.clone(), false, i == terms.len() - 1)?;
                    }
                }
            }
        }

        Ok(())
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_internal(f, "".to_string(), true, true)
    }
}
