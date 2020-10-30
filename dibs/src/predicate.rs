use std::fmt;
use std::fmt::Write;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ComparisonOperator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl fmt::Display for ComparisonOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_char(match self {
            ComparisonOperator::Eq => '=',
            ComparisonOperator::Ne => '≠',
            ComparisonOperator::Lt => '<',
            ComparisonOperator::Le => '≤',
            ComparisonOperator::Gt => '>',
            ComparisonOperator::Ge => '≥',
        })
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Value {
    Boolean(bool),
    Integer(usize),
    String(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Comparison {
    pub operator: ComparisonOperator,
    pub left: usize,
    pub right: usize,
}

impl Comparison {
    pub fn new(operator: ComparisonOperator, left: usize, right: usize) -> Comparison {
        Comparison {
            operator,
            left,
            right,
        }
    }
}

impl fmt::Display for Comparison {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "param_{} {} param_{}",
            self.left, self.operator, self.right
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Connective {
    Conjunction,
    Disjunction,
}

#[derive(Clone, Debug)]
pub enum Predicate {
    Comparison(Comparison),
    Connective(Connective, Vec<Predicate>),
}

impl Predicate {
    pub fn comparison(operator: ComparisonOperator, left: usize, right: usize) -> Predicate {
        Predicate::Comparison(Comparison::new(operator, left, right))
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

    // pub fn is_boolean(&self, v: bool) -> bool {
    //     match self {
    //         Predicate::Connective(connective, operands) => match (v, connective) {
    //             (true, Connective::Conjunction) | (false, Connective::Disjunction) => {
    //                 operands.is_empty()
    //             }
    //             _ => false,
    //         },
    //         _ => false,
    //     }
    // }

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
                    stack.push(node as *mut Predicate);
                } else {
                    for operand in operands {
                        stack.push(operand as *mut Predicate);
                    }
                }
            }
        }
    }

    pub fn is_normalized(&self) -> bool {
        match self {
            Predicate::Comparison(..) => true,
            Predicate::Connective(connective, operands) => match connective {
                Connective::Conjunction => operands.iter().all(|operand| match operand {
                    Predicate::Comparison(..) => true,
                    _ => false,
                }),
                Connective::Disjunction => operands.iter().all(|operand| match operand {
                    Predicate::Comparison(..) => true,
                    Predicate::Connective(sub_connective, sub_operands) => match sub_connective {
                        Connective::Conjunction => {
                            sub_operands.iter().all(|sub_operand| match sub_operand {
                                Predicate::Comparison(..) => true,
                                _ => false,
                            })
                        }
                        Connective::Disjunction => false,
                    },
                }),
            },
        }
    }

    pub fn normalize(&mut self) {
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

        self.condense();
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
            Predicate::Comparison(comparison) => {
                write!(f, "{}", comparison)?;
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
