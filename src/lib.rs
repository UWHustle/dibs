pub mod predicate;
mod solver;

#[cfg(test)]
mod tests {
    use crate::predicate::{Comparison, Operand, Predicate};
    use crate::solver;

    #[test]
    fn it_works() {
        let p = Predicate::disjunction(vec![
            Predicate::comparison(Comparison::Equal, Operand::Column(0), Operand::Variable(1)),
            Predicate::conjunction(vec![
                Predicate::comparison(Comparison::Equal, Operand::Column(1), Operand::Variable(2)),
                Predicate::comparison(Comparison::Equal, Operand::Column(1), Operand::Variable(3)),
            ]),
        ]);

        let q = Predicate::conjunction(vec![
            Predicate::comparison(Comparison::Equal, Operand::Column(0), Operand::Variable(4)),
            Predicate::comparison(Comparison::Equal, Operand::Column(1), Operand::Variable(5)),
        ]);

        println!("Predicate P:");
        println!("{}", p);
        println!();

        println!("Predicate Q:");
        println!("{}", q);
        println!();

        let r = solver::solve(p, q).condense();

        println!("Predicate R:");
        println!("{}", r);
    }
}
