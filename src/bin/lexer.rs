
use miette::Result;
use joujoudb::sql::parser::lexer::Lexer;

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<String>>();
    if args.len() > 1 {
        let lexer = Lexer::new(&args[1]);
        let tokens = lexer.into_iter().collect::<Result<Vec<_>>>();
        println!("{:?}", tokens?);
    }

    Ok(())
}
