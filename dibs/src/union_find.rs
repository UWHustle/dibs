use std::mem;

#[derive(Debug)]
struct Node {
    size: usize,
    parent: usize,
}

#[derive(Debug)]
pub struct UnionFind {
    nodes: Vec<Node>,
}

impl UnionFind {
    pub fn new(len: usize) -> UnionFind {
        UnionFind {
            nodes: (0..len).map(|i| Node { size: 1, parent: i }).collect(),
        }
    }

    pub fn union(&mut self, x: usize, y: usize) {
        let mut xf = self.find(x);
        let mut yf = self.find(y);

        if xf == yf {
            return;
        }

        if self.nodes[xf].size < self.nodes[yf].size {
            mem::swap(&mut xf, &mut yf);
        }

        self.nodes[yf].parent = xf;
        self.nodes[xf].size += self.nodes[yf].size;
    }

    pub fn find(&mut self, x: usize) -> usize {
        let mut xf = x;

        while self.nodes[xf].parent != xf {
            let xf_parent = self.nodes[xf].parent;
            self.nodes[xf].parent = self.nodes[xf_parent].parent;
            xf = xf_parent;
        }

        xf
    }

    pub fn sets(&mut self) -> Vec<Vec<usize>> {
        let mut sparse_sets = vec![vec![]; self.nodes.len()];

        for x in 0..self.nodes.len() {
            let xf = self.find(x);
            sparse_sets[xf].push(x);
        }

        sparse_sets.into_iter().filter(|s| !s.is_empty()).collect()
    }
}
