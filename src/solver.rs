use crate::predicate::{Comparison, Operator, Predicate};

pub struct Solver {
    p: Predicate,
    q: Predicate,
    stack: Vec<(Predicate, Predicate, *mut Predicate)>,
}

impl Solver {
    pub fn new(p: Predicate, q: Predicate) -> Self {
        Solver {
            p,
            q,
            stack: vec![],
        }
    }

    pub fn solve(&mut self) -> Predicate {
        let mut r = Predicate::with_value(false);

        self.stack
            .push((self.p.clone(), self.q.clone(), &mut r as *mut Predicate));

        while let Some((px, qx, rx)) = self.stack.pop() {
            match (px, qx) {
                (Predicate::Comparison(p_comparison), Predicate::Comparison(q_comparison)) => {
                    unsafe { *rx = self.conflict(&p_comparison, &q_comparison) };
                }

                (Predicate::Disjunction(other_terms), term)
                | (term, Predicate::Disjunction(other_terms)) => {
                    unsafe { *rx = Predicate::Conjunction(self.distribute(term, other_terms)) };
                }

                (Predicate::Conjunction(other_terms), term)
                | (term, Predicate::Conjunction(other_terms)) => {
                    unsafe { *rx = Predicate::Disjunction(self.distribute(term, other_terms)) };
                }
            }
        }

        r
    }

    fn distribute(&mut self, term: Predicate, other_terms: Vec<Predicate>) -> Vec<Predicate> {
        let mut empty = vec![Predicate::with_value(false); other_terms.len()];

        self.stack.extend(
            other_terms
                .into_iter()
                .zip(&mut empty)
                .map(|(child, empty_child)| (term.clone(), child, empty_child as *mut Predicate)),
        );

        empty
    }

    fn conflict(&self, p_comparison: &Comparison, q_comparison: &Comparison) -> Predicate {
        use Operator::*;

        if p_comparison.get_left() == q_comparison.get_left() {
            match (p_comparison.get_operator(), q_comparison.get_operator()) {
                (Equal, NotEqual) | (NotEqual, Equal) => Predicate::Comparison(Comparison::new(
                    Equal,
                    p_comparison.get_right(),
                    q_comparison.get_right(),
                )),

                (Equal, Equal) => Predicate::Comparison(Comparison::new(
                    NotEqual,
                    p_comparison.get_right(),
                    q_comparison.get_right(),
                )),

                (Equal, GreaterEqual) | (LessEqual, Equal) | (LessEqual, GreaterEqual) => {
                    Predicate::Comparison(Comparison::new(
                        Less,
                        p_comparison.get_right(),
                        q_comparison.get_right(),
                    ))
                }

                (Equal, Greater)
                | (Less, Equal)
                | (Less, Greater)
                | (LessEqual, Greater)
                | (Less, GreaterEqual) => Predicate::Comparison(Comparison::new(
                    LessEqual,
                    p_comparison.get_right(),
                    q_comparison.get_right(),
                )),

                (Equal, LessEqual) | (GreaterEqual, Equal) | (GreaterEqual, LessEqual) => {
                    Predicate::Comparison(Comparison::new(
                        Greater,
                        p_comparison.get_right(),
                        q_comparison.get_right(),
                    ))
                }

                (Equal, Less)
                | (Greater, Equal)
                | (Greater, Less)
                | (GreaterEqual, Less)
                | (Greater, LessEqual) => Predicate::Comparison(Comparison::new(
                    GreaterEqual,
                    p_comparison.get_right(),
                    q_comparison.get_right(),
                )),

                _ => Predicate::with_value(false),
            }
        } else {
            Predicate::with_value(false)
        }
    }
}
