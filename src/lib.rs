pub mod predicate;
mod solver;

#[cfg(test)]
mod tests {
    use crate::predicate::{Comparison, Operand, Predicate};
    use crate::solver::Solver;

    #[test]
    fn it_works() {
        let p = Predicate::conjunction(vec![
            Predicate::comparison(Comparison::Equal, Operand::Column(0), Operand::Variable(0)),
            Predicate::comparison(Comparison::Equal, Operand::Column(1), Operand::Variable(1)),
        ]);

        let q = Predicate::disjunction(vec![
            Predicate::comparison(Comparison::Equal, Operand::Column(0), Operand::Variable(2)),
            Predicate::comparison(Comparison::Equal, Operand::Column(1), Operand::Variable(3)),
        ]);

        println!("Predicate P:\n{}\n", p);

        println!("Predicate Q:\n{}\n", q);

        let mut s = Solver::new(p.clone(), q.clone());

        println!("Predicate R:\n{}", s.next().unwrap());
    }
}
