use crate::catalog::Catalog;
use crate::plan::logical::LogicalPlan;
use crate::table::Table;

trait PlanNode {
    fn next(&self) -> Option<()>;
}

struct Scan {
    table: Table,
}

impl PlanNode for Scan {
    fn next(&self) -> Option<()> {
        todo!()
    }
}

fn build(plan: &LogicalPlan, catalog: &Catalog) -> () {
    match plan {
        LogicalPlan::Projection { children } => todo!(),
        LogicalPlan::Filter { children } => todo!(),
        LogicalPlan::Scan { table } => Scan { table: todo!() },
    };
}

pub fn execute(logical_plan: &LogicalPlan, catalog: &Catalog) {
    let tree = build(logical_plan, catalog);

    // while let Some(batch) = tree.exe
}
