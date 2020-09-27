pub mod predicate;
mod solver;

#[cfg(test)]
mod tests {
    use crate::predicate::{Comparison, Operand, Operator, Predicate};
    use crate::solver::Solver;

    #[test]
    fn it_works() {
        let p = Predicate::Disjunction(vec![
            Predicate::Comparison(Comparison::new(
                Operator::Equal,
                Operand::Column(0),
                Operand::Variable(1),
            )),
            Predicate::Conjunction(vec![
                Predicate::Comparison(Comparison::new(
                    Operator::Greater,
                    Operand::Column(1),
                    Operand::Variable(2),
                )),
                Predicate::Comparison(Comparison::new(
                    Operator::Less,
                    Operand::Column(1),
                    Operand::Variable(3),
                )),
            ]),
        ]);

        let q = Predicate::Conjunction(vec![
            Predicate::Comparison(Comparison::new(
                Operator::Equal,
                Operand::Column(0),
                Operand::Variable(4),
            )),
            Predicate::Comparison(Comparison::new(
                Operator::Equal,
                Operand::Column(1),
                Operand::Variable(5),
            )),
        ]);

        println!("Predicate P:");
        println!("{}", p);
        println!();

        println!("Predicate Q:");
        println!("{}", q);
        println!();

        let mut solver = Solver::new(p, q);
        let mut r = solver.solve();
        r = r.condense();

        println!("Predicate R:");
        println!("{}", r);
    }
}
