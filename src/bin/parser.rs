use joujoudb::sql::parser::parser::Parser;
use miette::Result;

fn main() -> Result<()> {
    let mut buffer = String::new();
    let stdin = std::io::stdin();
    while stdin.read_line(&mut buffer).is_ok() && !buffer.is_empty() {
        let stmts = Parser::parse(&buffer)?;

        for stmt in stmts {
            println!("stmt={stmt:?}");
        }

        buffer.clear();
    }

    Ok(())
}
