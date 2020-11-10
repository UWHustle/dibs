use crate::predicate::{Comparison, Connective, Predicate, Value};
use crate::union_find::UnionFind;
use fnv::FnvHashMap;
use std::borrow::Cow;
use std::{mem, slice};

fn cluster<'a>(
    p: &'a Predicate,
    q: &'a Predicate,
) -> impl Iterator<Item = (Predicate, Predicate)> + 'a {
    let p_conjuncts = match p {
        Predicate::Connective(_p_connective @ Connective::Conjunction, p_operands) => p_operands,
        _ => slice::from_ref(p),
    };

    let q_conjuncts = match q {
        Predicate::Connective(_q_connective @ Connective::Conjunction, q_operands) => q_operands,
        _ => slice::from_ref(q),
    };

    let mut column_map = FnvHashMap::default();
    let mut union_find = UnionFind::new(p_conjuncts.len() + q_conjuncts.len());

    for (i, conjunct) in p_conjuncts.iter().chain(q_conjuncts).enumerate() {
        for node in conjunct.preorder() {
            if let Predicate::Comparison(comparison) = node {
                let j = *column_map.entry(comparison.left).or_insert(i);

                if i != j {
                    union_find.union(i, j);
                }
            }
        }
    }

    union_find.sets().into_iter().map(move |indices| {
        let mut p_sub = vec![];
        let mut q_sub = vec![];

        for i in indices {
            if i < p_conjuncts.len() {
                p_sub.push(p_conjuncts[i].clone());
            } else {
                q_sub.push(q_conjuncts[i - p_conjuncts.len()].clone());
            }
        }

        (Predicate::conjunction(p_sub), Predicate::conjunction(q_sub))
    })
}

fn prepare_comparison_comparison(p: &Comparison, q: &Comparison, swap: bool) -> Predicate {
    use crate::predicate::ComparisonOperator::*;

    if p.left != q.left {
        return Predicate::boolean(true);
    }

    let mut p_ref = Cow::Borrowed(p);
    let mut q_ref = Cow::Borrowed(q);

    if swap {
        mem::swap(p_ref.to_mut(), q_ref.to_mut());
    }

    match (p_ref.operator, q_ref.operator) {
        (Eq, Eq) => Predicate::comparison(Eq, p_ref.right, q_ref.right),
        (Eq, Ne) | (Ne, Eq) => Predicate::comparison(Ne, p_ref.right, q_ref.right),
        (Eq, Lt) | (Gt, Eq) | (Gt, Lt) | (Ge, Lt) | (Gt, Le) => {
            Predicate::comparison(Lt, p_ref.right, q_ref.right)
        }
        (Eq, Le) | (Ge, Eq) | (Ge, Le) => Predicate::comparison(Le, p_ref.right, q_ref.right),
        (Eq, Gt) | (Lt, Eq) | (Lt, Gt) | (Le, Gt) | (Lt, Ge) => {
            Predicate::comparison(Gt, p_ref.right, q_ref.right)
        }
        (Eq, Ge) | (Le, Eq) | (Le, Ge) => Predicate::comparison(Ge, p_ref.right, q_ref.right),
        _ => Predicate::boolean(true),
    }
}

fn prepare_comparison_conjunction(p: &Comparison, q: &[Predicate], swap: bool) -> Predicate {
    Predicate::conjunction(
        q.iter()
            .filter_map(|q_conjunct| match q_conjunct {
                Predicate::Comparison(q_comparison) => {
                    Some(prepare_comparison_comparison(p, q_comparison, swap))
                }
                _ => None,
            })
            .collect(),
    )
}

fn prepare_comparison_disjunction(p: &Comparison, q: &[Predicate], swap: bool) -> Predicate {
    Predicate::disjunction(
        q.iter()
            .filter_map(|q_disjunct| match q_disjunct {
                Predicate::Comparison(q_comparison) => {
                    Some(prepare_comparison_comparison(p, q_comparison, swap))
                }
                Predicate::Connective(_q_connective @ Connective::Conjunction, q_operands) => {
                    Some(prepare_comparison_conjunction(p, q_operands, swap))
                }
                _ => None,
            })
            .collect(),
    )
}

fn prepare_conjunction_comparison(p: &[Predicate], q: &Comparison, swap: bool) -> Predicate {
    prepare_comparison_conjunction(q, p, !swap)
}

fn prepare_conjunction_conjunction(p: &[Predicate], q: &[Predicate], swap: bool) -> Predicate {
    Predicate::conjunction(
        p.iter()
            .filter_map(|p_conjunct| match p_conjunct {
                Predicate::Comparison(p_comparison) => {
                    Some(prepare_comparison_conjunction(p_comparison, q, swap))
                }
                _ => None,
            })
            .collect(),
    )
}

fn prepare_conjunction_disjunction(p: &[Predicate], q: &[Predicate], swap: bool) -> Predicate {
    Predicate::disjunction(
        q.iter()
            .filter_map(|q_disjunct| match q_disjunct {
                Predicate::Comparison(q_comparison) => {
                    Some(prepare_conjunction_comparison(p, q_comparison, swap))
                }
                Predicate::Connective(_q_connective @ Connective::Conjunction, q_operands) => {
                    Some(prepare_conjunction_conjunction(p, q_operands, swap))
                }
                _ => None,
            })
            .collect(),
    )
}

fn prepare_disjunction_comparison(p: &[Predicate], q: &Comparison, swap: bool) -> Predicate {
    prepare_comparison_disjunction(q, p, !swap)
}

fn prepare_disjunction_conjunction(p: &[Predicate], q: &[Predicate], swap: bool) -> Predicate {
    prepare_conjunction_disjunction(q, p, !swap)
}

fn prepare_disjunction_disjunction(p: &[Predicate], q: &[Predicate], swap: bool) -> Predicate {
    Predicate::disjunction(
        p.iter()
            .filter_map(|p_disjunct| match p_disjunct {
                Predicate::Comparison(p_comparison) => {
                    Some(prepare_comparison_disjunction(p_comparison, q, swap))
                }
                Predicate::Connective(_p_connective @ Connective::Conjunction, p_operands) => {
                    Some(prepare_conjunction_disjunction(p_operands, q, swap))
                }
                _ => None,
            })
            .collect(),
    )
}

fn solve_comparison_comparison(
    p: &Comparison,
    p_args: &[Value],
    q: &Comparison,
    q_args: &[Value],
) -> bool {
    use crate::predicate::ComparisonOperator::*;

    if p.left != q.left {
        return true;
    }

    let p_value = &p_args[p.right];
    let q_value = &q_args[q.right];

    assert_eq!(
        mem::discriminant(p_value),
        mem::discriminant(q_value),
        "cannot solve comparisons between different types"
    );

    match (p.operator, q.operator) {
        (Eq, Eq) => p_value == q_value,
        (Eq, Ne) | (Ne, Eq) => p_value != q_value,
        (Eq, Lt) | (Gt, Eq) | (Gt, Lt) | (Ge, Lt) | (Gt, Le) => p_value < q_value,
        (Eq, Le) | (Ge, Eq) | (Ge, Le) => p_value <= q_value,
        (Eq, Gt) | (Lt, Eq) | (Lt, Gt) | (Le, Gt) | (Lt, Ge) => p_value > q_value,
        (Eq, Ge) | (Le, Eq) | (Le, Ge) => p_value >= q_value,
        _ => true,
    }
}

fn solve_comparison_conjunction(
    p: &Comparison,
    p_args: &[Value],
    q: &[Predicate],
    q_args: &[Value],
) -> bool {
    q.iter().all(|q_conjunct| match q_conjunct {
        Predicate::Comparison(q_comparison) => {
            solve_comparison_comparison(p, p_args, q_comparison, q_args)
        }
        _ => true,
    })
}

fn solve_comparison_disjunction(
    p: &Comparison,
    p_args: &[Value],
    q: &[Predicate],
    q_args: &[Value],
) -> bool {
    q.iter().any(|q_disjunct| match q_disjunct {
        Predicate::Comparison(q_comparison) => {
            solve_comparison_comparison(p, p_args, q_comparison, q_args)
        }
        Predicate::Connective(_q_connective @ Connective::Conjunction, q_operands) => {
            solve_comparison_conjunction(p, p_args, q_operands, q_args)
        }
        _ => true,
    })
}

fn solve_conjunction_comparison(
    p: &[Predicate],
    p_args: &[Value],
    q: &Comparison,
    q_args: &[Value],
) -> bool {
    solve_comparison_conjunction(q, q_args, p, p_args)
}

fn solve_conjunction_conjunction(
    p: &[Predicate],
    p_args: &[Value],
    q: &[Predicate],
    q_args: &[Value],
) -> bool {
    p.iter().all(|p_conjunct| match p_conjunct {
        Predicate::Comparison(p_comparison) => {
            solve_comparison_conjunction(p_comparison, p_args, q, q_args)
        }
        _ => true,
    })
}

fn solve_conjunction_disjunction(
    p: &[Predicate],
    p_args: &[Value],
    q: &[Predicate],
    q_args: &[Value],
) -> bool {
    q.iter().any(|q_disjunct| match q_disjunct {
        Predicate::Comparison(q_comparison) => {
            solve_conjunction_comparison(p, p_args, q_comparison, q_args)
        }
        Predicate::Connective(_q_connective @ Connective::Conjunction, q_operands) => {
            solve_conjunction_conjunction(p, p_args, q_operands, q_args)
        }
        _ => true,
    })
}

fn solve_disjunction_comparison(
    p: &[Predicate],
    p_args: &[Value],
    q: &Comparison,
    q_args: &[Value],
) -> bool {
    solve_comparison_disjunction(q, q_args, p, p_args)
}

fn solve_disjunction_conjunction(
    p: &[Predicate],
    p_args: &[Value],
    q: &[Predicate],
    q_args: &[Value],
) -> bool {
    solve_conjunction_disjunction(q, q_args, p, p_args)
}

fn solve_disjunction_disjunction(
    p: &[Predicate],
    p_args: &[Value],
    q: &[Predicate],
    q_args: &[Value],
) -> bool {
    p.iter().any(|p_disjunct| match p_disjunct {
        Predicate::Comparison(p_comparison) => {
            solve_comparison_disjunction(p_comparison, p_args, q, q_args)
        }
        Predicate::Connective(_p_connective @ Connective::Conjunction, p_operands) => {
            solve_conjunction_disjunction(p_operands, p_args, q, q_args)
        }
        _ => true,
    })
}

fn dnf_blowup(p: &Predicate) -> usize {
    match p {
        Predicate::Comparison(_) => 1,
        Predicate::Connective(connective, operands) => match connective {
            Connective::Conjunction => operands.iter().fold(1, |acc, x| acc * dnf_blowup(x)),
            Connective::Disjunction => operands.iter().fold(0, |acc, x| acc + dnf_blowup(x)),
        },
    }
}

pub fn prepare(p: &Predicate, q: &Predicate) -> Predicate {
    let mut r = Predicate::conjunction(
        cluster(p, q)
            .map(|(mut p_conjunct, mut q_conjunct)| {
                p_conjunct.normalize();
                q_conjunct.normalize();

                match (&p_conjunct, &q_conjunct) {
                    (Predicate::Comparison(p_comparison), Predicate::Comparison(q_comparison)) => {
                        prepare_comparison_comparison(p_comparison, q_comparison, false)
                    }
                    (
                        Predicate::Comparison(p_comparison),
                        Predicate::Connective(q_connective, q_operands),
                    ) => match q_connective {
                        Connective::Conjunction => {
                            prepare_comparison_conjunction(p_comparison, q_operands, false)
                        }
                        Connective::Disjunction => {
                            prepare_comparison_disjunction(p_comparison, q_operands, false)
                        }
                    },
                    (
                        Predicate::Connective(p_connective, p_operands),
                        Predicate::Comparison(q_comparison),
                    ) => match p_connective {
                        Connective::Conjunction => {
                            prepare_conjunction_comparison(p_operands, q_comparison, false)
                        }
                        Connective::Disjunction => {
                            prepare_disjunction_comparison(p_operands, q_comparison, false)
                        }
                    },
                    (
                        Predicate::Connective(p_connective, p_operands),
                        Predicate::Connective(q_connective, q_operands),
                    ) => match (p_connective, q_connective) {
                        (Connective::Conjunction, Connective::Conjunction) => {
                            prepare_conjunction_conjunction(p_operands, q_operands, false)
                        }
                        (Connective::Conjunction, Connective::Disjunction) => {
                            prepare_conjunction_disjunction(p_operands, q_operands, false)
                        }
                        (Connective::Disjunction, Connective::Conjunction) => {
                            prepare_disjunction_conjunction(p_operands, q_operands, false)
                        }
                        (Connective::Disjunction, Connective::Disjunction) => {
                            prepare_disjunction_disjunction(p_operands, q_operands, false)
                        }
                    },
                }
            })
            .collect(),
    );

    r.condense();

    r
}

pub fn evaluate(conflict: &Predicate, p_args: &[Value], q_args: &[Value]) -> bool {
    use crate::predicate::ComparisonOperator::*;

    match conflict {
        Predicate::Comparison(comparison) => match comparison.operator {
            Eq => p_args[comparison.left] == q_args[comparison.right],
            Ne => p_args[comparison.left] != q_args[comparison.right],
            Lt => p_args[comparison.left] < q_args[comparison.right],
            Le => p_args[comparison.left] <= q_args[comparison.right],
            Gt => p_args[comparison.left] > q_args[comparison.right],
            Ge => p_args[comparison.left] >= q_args[comparison.right],
        },
        Predicate::Connective(connective, operands) => match connective {
            Connective::Conjunction => operands
                .iter()
                .all(|operand| evaluate(operand, p_args, q_args)),
            Connective::Disjunction => operands
                .iter()
                .any(|operand| evaluate(operand, p_args, q_args)),
        },
    }
}

pub fn solve_dnf(
    p: &Predicate,
    p_args: &[Value],
    q: &Predicate,
    q_args: &[Value],
    blowup_limit: usize,
) -> bool {
    if dnf_blowup(p) * dnf_blowup(q) > blowup_limit {
        return true;
    }

    let mut p_dnf = Cow::Borrowed(p);
    if !p_dnf.is_normalized() {
        p_dnf.to_mut().normalize();
    }

    let mut q_dnf = Cow::Borrowed(q);
    if !q_dnf.is_normalized() {
        q_dnf.to_mut().normalize();
    }

    match (&*p_dnf, &*q_dnf) {
        (Predicate::Comparison(p_comparison), Predicate::Comparison(q_comparison)) => {
            solve_comparison_comparison(p_comparison, p_args, q_comparison, q_args)
        }
        (Predicate::Comparison(p_comparison), Predicate::Connective(q_connective, q_operands)) => {
            match q_connective {
                Connective::Conjunction => {
                    solve_comparison_conjunction(p_comparison, p_args, q_operands, q_args)
                }
                Connective::Disjunction => {
                    solve_comparison_disjunction(p_comparison, p_args, q_operands, q_args)
                }
            }
        }
        (Predicate::Connective(p_connective, p_operands), Predicate::Comparison(q_comparison)) => {
            match p_connective {
                Connective::Conjunction => {
                    solve_conjunction_comparison(p_operands, p_args, q_comparison, q_args)
                }
                Connective::Disjunction => {
                    solve_disjunction_comparison(p_operands, p_args, q_comparison, q_args)
                }
            }
        }
        (
            Predicate::Connective(p_connective, p_operands),
            Predicate::Connective(q_connective, q_operands),
        ) => match (p_connective, q_connective) {
            (Connective::Conjunction, Connective::Conjunction) => {
                solve_conjunction_conjunction(p_operands, p_args, q_operands, q_args)
            }
            (Connective::Conjunction, Connective::Disjunction) => {
                solve_conjunction_disjunction(p_operands, p_args, q_operands, q_args)
            }
            (Connective::Disjunction, Connective::Conjunction) => {
                solve_disjunction_conjunction(p_operands, p_args, q_operands, q_args)
            }
            (Connective::Disjunction, Connective::Disjunction) => {
                solve_disjunction_disjunction(p_operands, p_args, q_operands, q_args)
            }
        },
    }
}

pub fn solve_clustered(
    p: &Predicate,
    p_args: &[Value],
    q: &Predicate,
    q_args: &[Value],
    blowup_limit: usize,
) -> bool {
    cluster(&p, &q).all(|(p_conjunct, q_conjunct)| {
        solve_dnf(&p_conjunct, p_args, &q_conjunct, q_args, blowup_limit)
    })
}
