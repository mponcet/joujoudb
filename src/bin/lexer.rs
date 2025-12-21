use joujoudb::sql::parser::lexer::Lexer;
use miette::Result;

fn main() -> Result<()> {
    let mut buffer = String::new();
    let stdin = std::io::stdin();
    while stdin.read_line(&mut buffer).is_ok() {
        let lexer = Lexer::new(&buffer);
        let tokens = lexer.into_iter().collect::<Result<Vec<_>>>();
        println!("{:?}", tokens?);
        buffer.clear();
    }

    Ok(())
}
