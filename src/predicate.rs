use std::fmt;
use std::fmt::Write;

#[derive(Clone)]
pub enum Predicate {
    Comparison(Comparison),
    Conjunction(Vec<Predicate>),
    Disjunction(Vec<Predicate>),
}

impl Predicate {
    pub fn with_value(v: bool) -> Self {
        if v {
            Predicate::Conjunction(Default::default())
        } else {
            Predicate::Disjunction(Default::default())
        }
    }

    pub fn condense(self) -> Self {
        match self {
            Predicate::Conjunction(mut terms) => {
                let mut new_terms = vec![];

                while let Some(term) = terms.pop() {
                    let mut new_term = term.condense();
                    match new_term {
                        Predicate::Conjunction(ref mut sub_terms) => new_terms.append(sub_terms),
                        Predicate::Disjunction(sub_terms) => {
                            if (sub_terms).is_empty() {
                                return Predicate::with_value(false);
                            }

                            new_terms.push(Predicate::Disjunction(sub_terms));
                        }
                        comparison => new_terms.push(comparison),
                    }
                }

                if new_terms.len() == 1 {
                    return new_terms.pop().unwrap();
                }

                Predicate::Conjunction(new_terms)
            }
            Predicate::Disjunction(mut terms) => {
                let mut new_terms = vec![];

                while let Some(term) = terms.pop() {
                    let mut new_term = term.condense();
                    match new_term {
                        Predicate::Conjunction(sub_terms) => {
                            if sub_terms.is_empty() {
                                return Predicate::with_value(true);
                            }

                            new_terms.push(Predicate::Conjunction(sub_terms));
                        }
                        Predicate::Disjunction(ref mut sub_terms) => new_terms.append(sub_terms),
                        comparison => new_terms.push(comparison),
                    }
                }

                if new_terms.len() == 1 {
                    return new_terms.pop().unwrap();
                }

                Predicate::Disjunction(new_terms)
            }
            _ => self,
        }
    }

    fn to_string_internal(&self, mut indent: String, first: bool, last: bool) -> String {
        let mut out = indent.clone();

        if !first && last {
            out += "└── ";
            indent += "    ";
        } else if !last {
            out += "├── ";
            indent += "│   ";
        }

        match self {
            Predicate::Comparison(comparison) => {
                out += &format!("{}\n", comparison.to_string());
            }
            Predicate::Conjunction(terms) => {
                if terms.is_empty() {
                    out += "true\n";
                } else {
                    out += "AND\n";
                    for i in 0..terms.len() {
                        out += &terms[i].to_string_internal(
                            indent.clone(),
                            false,
                            i == terms.len() - 1,
                        );
                    }
                }
            }
            Predicate::Disjunction(terms) => {
                if terms.is_empty() {
                    out += "false\n";
                } else {
                    out += "OR\n";
                    for i in 0..terms.len() {
                        out += &terms[i].to_string_internal(
                            indent.clone(),
                            false,
                            i == terms.len() - 1,
                        );
                    }
                }
            }
        }

        out
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut s = self.to_string_internal("".to_string(), true, true);
        s.pop(); // Remove the trailing newline
        f.write_str(&s)
    }
}

#[derive(Clone)]
pub struct Comparison {
    operator: Operator,
    left: Operand,
    right: Operand,
}

impl Comparison {
    pub fn new(operator: Operator, left: Operand, right: Operand) -> Self {
        Comparison {
            operator,
            left,
            right,
        }
    }

    pub fn get_operator(&self) -> Operator {
        self.operator
    }

    pub fn get_left(&self) -> Operand {
        self.left.clone()
    }

    pub fn get_right(&self) -> Operand {
        self.right.clone()
    }
}

impl fmt::Display for Comparison {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.operator, self.right)
    }
}

#[derive(Clone, Copy)]
pub enum Operator {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_char(match self {
            Operator::Equal => '=',
            Operator::NotEqual => '≠',
            Operator::Less => '<',
            Operator::LessEqual => '≤',
            Operator::Greater => '>',
            Operator::GreaterEqual => '≥',
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
