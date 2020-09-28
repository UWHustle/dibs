use crate::predicate::{Comparison::*, Predicate};

pub fn solve(p: Predicate, q: Predicate) -> Predicate {
    let mut r = Predicate::boolean(true);
    let mut stack = vec![(p, q, &mut r as *mut Predicate)];

    while let Some((px, qx, rx)) = stack.pop() {
        match (px, qx) {
            (
                Predicate::Comparison(p_comparison, p_left, p_right),
                Predicate::Comparison(q_comparison, q_left, q_right),
            ) => {
                if p_left == q_left {
                    let r_comparison_option = match (p_comparison, q_comparison) {
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
                    };

                    if let Some(r_comparison) = r_comparison_option {
                        unsafe { *rx = Predicate::comparison(r_comparison, p_right, q_right) };
                    }
                }
            }
            (Predicate::Connective(connective, terms), other)
            | (other, Predicate::Connective(connective, terms)) => {
                let mut empty_terms = vec![Predicate::boolean(true); terms.len()];

                stack.extend(
                    terms
                        .into_iter()
                        .zip(&mut empty_terms)
                        .map(|(term, empty_term)| {
                            (other.clone(), term, empty_term as *mut Predicate)
                        }),
                );

                unsafe { *rx = Predicate::Connective(connective, empty_terms) };
            }
        };
    }

    r
}
