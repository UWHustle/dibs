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

#[derive(Clone, Copy, Debug, PartialEq)]
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
    pub fn comparison(comparison: Comparison, left: Operand, right: Operand) -> Predicate {
        Predicate::Comparison(comparison, left, right)
    }

    pub fn conjunction(operands: Vec<Predicate>) -> Predicate {
        Predicate::Connective(Connective::Conjunction, operands)
    }

    pub fn disjunction(operands: Vec<Predicate>) -> Predicate {
        Predicate::Connective(Connective::Disjunction, operands)
    }

    pub fn boolean(v: bool) -> Predicate {
        if v {
            Predicate::conjunction(vec![])
        } else {
            Predicate::disjunction(vec![])
        }
    }

    pub fn condense(&mut self) {
        let mut stack = vec![self as *mut Predicate];

        while let Some(node_ptr) = stack.pop() {
            let node = unsafe { &mut *node_ptr };

            if let Predicate::Connective(connective, operands) = node {
                let mut i = 0;
                while i < operands.len() {
                    match operands[i] {
                        Predicate::Connective(sub_connective, _)
                            if sub_connective == *connective =>
                        {
                            if let Predicate::Connective(_, sub_operands) = operands.swap_remove(i)
                            {
                                operands.extend(sub_operands);
                            }
                        }
                        Predicate::Connective(_, ref sub_operands) if sub_operands.is_empty() => {
                            operands.clear();
                        }
                        _ => i += 1,
                    }
                }

                if operands.len() == 1 {
                    *node = operands.pop().unwrap();
                } else {
                    for operand in operands {
                        stack.push(operand as *mut Predicate);
                    }
                }
            }
        }
    }

    pub fn disjunctive_normalize(&mut self) {
        let mut stack = vec![self as *mut Predicate];

        while let Some(node_ptr) = stack.pop() {
            let node = unsafe { &mut *node_ptr };

            if let Predicate::Connective(connective, operands) = node {
                if *connective == Connective::Conjunction {
                    let disjunction_position = operands.iter().position(|operand| match operand {
                        Predicate::Connective(_sub_connective @ Connective::Disjunction, _) => true,
                        _ => false,
                    });

                    if let Some(i) = disjunction_position {
                        let disjunction = operands.swap_remove(i);
                        let mut new_operands = vec![];

                        if let Predicate::Connective(_, disjunction_operands) = disjunction {
                            for disjunction_operand in disjunction_operands {
                                let mut conjunction_operands = operands.clone();
                                conjunction_operands.push(disjunction_operand);
                                new_operands.push(Predicate::conjunction(conjunction_operands));
                            }
                        }

                        *connective = Connective::Disjunction;
                        *operands = new_operands;
                    }
                }

                for operand in operands {
                    stack.push(operand as *mut Predicate);
                }
            }
        }
    }

    pub fn preorder(&self) -> PreorderIter {
        PreorderIter::new(self)
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
            Predicate::Connective(connective, operands) => {
                if operands.is_empty() {
                    match connective {
                        Connective::Conjunction => f.write_str("TRUE")?,
                        Connective::Disjunction => f.write_str("FALSE")?,
                    }
                } else {
                    match connective {
                        Connective::Conjunction => f.write_str("AND")?,
                        Connective::Disjunction => f.write_str("OR")?,
                    }

                    for i in 0..operands.len() {
                        f.write_char('\n')?;
                        operands[i].fmt_internal(
                            f,
                            indent.clone(),
                            false,
                            i == operands.len() - 1,
                        )?;
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

pub struct PreorderIter<'a> {
    stack: Vec<&'a Predicate>,
}

impl<'a> PreorderIter<'a> {
    fn new(p: &'a Predicate) -> PreorderIter {
        PreorderIter { stack: vec![p] }
    }
}

impl<'a> Iterator for PreorderIter<'a> {
    type Item = &'a Predicate;

    fn next(&mut self) -> Option<&'a Predicate> {
        let node = self.stack.pop()?;

        if let Predicate::Connective(_, operands) = node {
            for operand in operands.iter().rev() {
                self.stack.push(operand);
            }
        }

        Some(node)
    }
}
