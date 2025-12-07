pub(super) trait PeekableExt<I>: Iterator
where
    I: Iterator,
{
    fn peekable_take_while<P>(&mut self, predicate: P) -> PeekableTakeWhile<'_, I, P>
    where
        P: Fn(&I::Item) -> bool;
}

impl<I: Iterator> PeekableExt<I> for core::iter::Peekable<I> {
    fn peekable_take_while<P>(&mut self, predicate: P) -> PeekableTakeWhile<'_, I, P>
    where
        P: Fn(&I::Item) -> bool,
    {
        PeekableTakeWhile {
            iter: self,
            predicate,
        }
    }
}

pub(super) struct PeekableTakeWhile<'iter, I, P>
where
    I: Iterator,
    P: Fn(&I::Item) -> bool,
{
    iter: &'iter mut core::iter::Peekable<I>,
    predicate: P,
}

impl<I, P> Iterator for PeekableTakeWhile<'_, I, P>
where
    I: Iterator,
    P: Fn(&I::Item) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next_if(&self.predicate)
    }
}
