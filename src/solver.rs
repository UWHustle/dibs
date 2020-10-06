use crate::predicate::{Comparison, Comparison::*, Connective, Operand, Predicate};
use fnv::FnvHashSet;
use std::collections::VecDeque;
use std::iter::FromIterator;
use std::{mem, slice};

pub struct Solver {
    pq: Option<(Predicate, Predicate)>,
    queue: VecDeque<(Predicate, Predicate)>,
}

impl Solver {
    pub fn new(p: Predicate, q: Predicate) -> Solver {
        let mut queue = VecDeque::new();

        for p_node in match &p {
            Predicate::Connective(_connective @ Connective::Conjunction, operands) => operands,
            _ => slice::from_ref(&p),
        } {
            if let Predicate::Comparison(_, p_left, _) = p_node {
                for q_node in match &q {
                    Predicate::Connective(_connective @ Connective::Conjunction, operands) => {
                        operands
                    }
                    _ => slice::from_ref(&q),
                } {
                    if let Predicate::Comparison(_, q_left, _) = q_node {
                        if p_left == q_left {
                            queue.push_back((p_node.clone(), q_node.clone()));
                        }
                    }
                }
            }
        }

        Solver {
            pq: Some((p, q)),
            queue,
        }
    }

    fn solve_comparisons(
        p_comparison: Comparison,
        p_left: Operand,
        p_right: Operand,
        q_comparison: Comparison,
        q_left: Operand,
        q_right: Operand,
    ) -> Predicate {
        if p_left != q_left {
            return Predicate::boolean(true);
        }

        match (p_comparison, q_comparison) {
            (Equal, Equal) => Some(Equal),

            (Equal, NotEqual) | (NotEqual, Equal) => Some(NotEqual),

            (Equal, Less)
            | (Greater, Equal)
            | (Greater, Less)
            | (GreaterEqual, Less)
            | (Greater, LessEqual) => Some(Less),

            (Equal, LessEqual) | (GreaterEqual, Equal) | (GreaterEqual, LessEqual) => {
                Some(LessEqual)
            }

            (Equal, Greater)
            | (Less, Equal)
            | (Less, Greater)
            | (LessEqual, Greater)
            | (Less, GreaterEqual) => Some(Greater),

            (Equal, GreaterEqual) | (LessEqual, Equal) | (LessEqual, GreaterEqual) => {
                Some(GreaterEqual)
            }

            _ => None,
        }
        .map(|comparison| Predicate::Comparison(comparison, p_right, q_right))
        .unwrap_or(Predicate::boolean(true))
    }

    fn solve_conjunctions(p: &[Predicate], q: &[Predicate]) -> Predicate {
        let mut r_operands = vec![];

        for p_operand in p {
            for q_operand in q {
                let r_operand = match (p_operand, q_operand) {
                    (
                        Predicate::Comparison(p_comparison, p_left, p_right),
                        Predicate::Comparison(q_comparison, q_left, q_right),
                    ) => Solver::solve_comparisons(
                        *p_comparison,
                        *p_left,
                        *p_right,
                        *q_comparison,
                        *q_left,
                        *q_right,
                    ),
                    _ => panic!("predicate is not in condensed disjunctive normal form"),
                };

                r_operands.push(r_operand);
            }
        }

        Predicate::conjunction(r_operands)
    }

    fn solve_disjunctions(p: &[Predicate], q: &[Predicate]) -> Predicate {
        let mut r_operands = vec![];

        for p_operand in p {
            for q_operand in q {
                let r_operand = match (p_operand, q_operand) {
                    (
                        Predicate::Comparison(p_comparison, p_left, p_right),
                        Predicate::Comparison(q_comparison, q_left, q_right),
                    ) => Solver::solve_comparisons(
                        *p_comparison,
                        *p_left,
                        *p_right,
                        *q_comparison,
                        *q_left,
                        *q_right,
                    ),
                    _ => Solver::solve_conjunctions(
                        Solver::conjuncts(p_operand),
                        Solver::conjuncts(q_operand),
                    ),
                };

                r_operands.push(r_operand)
            }
        }

        Predicate::disjunction(r_operands)
    }

    fn conjuncts(p: &Predicate) -> &[Predicate] {
        match p {
            Predicate::Connective(_connective @ Connective::Conjunction, operands) => operands,
            _ => slice::from_ref(p),
        }
    }

    fn column_sets(conjuncts: &[Predicate]) -> Vec<FnvHashSet<usize>> {
        conjuncts
            .iter()
            .map(|conjunct| match conjunct {
                Predicate::Comparison(_, left, right) => {
                    assert_eq!(
                        mem::discriminant(right),
                        mem::discriminant(&Operand::Variable(Default::default())),
                        "operands on the right-hand side of comparisons must be variables"
                    );

                    let mut column_set = FnvHashSet::default();

                    column_set.insert(match left {
                        Operand::Column(id) => *id,
                        _ => {
                            panic!("operands on the left-hand side of comparisons must be columns")
                        }
                    });

                    column_set
                }

                Predicate::Connective(connective, _) => {
                    assert_eq!(
                        *connective,
                        Connective::Disjunction,
                        "predicate must be condensed conjunctive normal form"
                    );

                    FnvHashSet::from_iter(conjunct.preorder().filter_map(|node| match node {
                        Predicate::Comparison(_, left, right) => {
                            assert_eq!(
                                mem::discriminant(right),
                                mem::discriminant(&Operand::Variable(Default::default())),
                                "operands on the right-hand side of comparisons must be variables"
                            );

                            match left {
                                Operand::Column(id) => Some(*id),
                                _ => panic!(
                                    "operands on the left-hand side of comparisons must be columns"
                                ),
                            }
                        }
                        _ => None,
                    }))
                }
            })
            .collect()
    }

    fn intersecting(
        column_set: &FnvHashSet<usize>,
        other_column_sets: &[FnvHashSet<usize>],
    ) -> Vec<usize> {
        other_column_sets
            .iter()
            .enumerate()
            .filter(|(_, other_column_set)| !other_column_set.is_disjoint(column_set))
            .map(|(i, _)| i)
            .collect()
    }
}

impl Iterator for Solver {
    type Item = Predicate;

    fn next(&mut self) -> Option<Predicate> {
        if self.queue.is_empty() {
            if let Some((p, q)) = self.pq.take() {
                let p_conjuncts = Solver::conjuncts(&p);
                let q_conjuncts = Solver::conjuncts(&q);

                let p_column_sets = Solver::column_sets(p_conjuncts);
                let q_column_sets = Solver::column_sets(q_conjuncts);

                for (p_conjunct, p_column_set) in p_conjuncts.iter().zip(&p_column_sets) {
                    if let Predicate::Connective(..) = p_conjunct {
                        let q_is = Solver::intersecting(&p_column_set, &q_column_sets);

                        if q_is.len() == 1 {
                            self.queue
                                .push_back((p_conjunct.clone(), q_conjuncts[q_is[0]].clone()));
                        } else if q_is.len() > 1 {
                            self.queue.push_back((
                                p_conjunct.clone(),
                                Predicate::conjunction(
                                    q_is.iter().map(|q_i| q_conjuncts[*q_i].clone()).collect(),
                                ),
                            ))
                        }
                    }
                }

                for (q_conjunct, q_column_set) in q_conjuncts.iter().zip(&q_column_sets) {
                    if let Predicate::Connective(..) = q_conjunct {
                        let p_is = Solver::intersecting(q_column_set, &p_column_sets);

                        if p_is.len() > 1 {
                            self.queue.push_back((
                                Predicate::conjunction(
                                    p_is.iter().map(|p_i| p_conjuncts[*p_i].clone()).collect(),
                                ),
                                q_conjunct.clone(),
                            ))
                        }
                    }
                }
            }
        }

        while let Some((p, q)) = self.queue.pop_front() {
            let mut r = match &p {
                Predicate::Comparison(p_comparison, p_left, p_right) => match &q {
                    Predicate::Comparison(q_comparison, q_left, q_right) => {
                        Solver::solve_comparisons(
                            *p_comparison,
                            *p_left,
                            *p_right,
                            *q_comparison,
                            *q_left,
                            *q_right,
                        )
                    }
                    Predicate::Connective(q_connective, q_operands) => match q_connective {
                        Connective::Conjunction => {
                            Solver::solve_conjunctions(slice::from_ref(&p), q_operands)
                        }
                        Connective::Disjunction => {
                            Solver::solve_disjunctions(slice::from_ref(&p), q_operands)
                        }
                    },
                },

                Predicate::Connective(p_connective, p_operands) => match &q {
                    Predicate::Comparison(..) => match p_connective {
                        Connective::Conjunction => {
                            Solver::solve_conjunctions(p_operands, slice::from_ref(&q))
                        }
                        Connective::Disjunction => {
                            Solver::solve_disjunctions(p_operands, slice::from_ref(&q))
                        }
                    },

                    Predicate::Connective(q_connective, q_operands) => {
                        match (p_connective, q_connective) {
                            (Connective::Conjunction, Connective::Conjunction) => {
                                Solver::solve_conjunctions(p_operands, q_operands)
                            }
                            (Connective::Conjunction, Connective::Disjunction) => {
                                Solver::solve_disjunctions(slice::from_ref(&p), q_operands)
                            }
                            (Connective::Disjunction, Connective::Conjunction) => {
                                Solver::solve_disjunctions(p_operands, slice::from_ref(&q))
                            }
                            (Connective::Disjunction, Connective::Disjunction) => {
                                Solver::solve_disjunctions(p_operands, q_operands)
                            }
                        }
                    }
                },
            };

            r.condense();
            match r {
                Predicate::Connective(_, operands) if operands.is_empty() => (),
                _ => return Some(r),
            }
        }

        None
    }
}
