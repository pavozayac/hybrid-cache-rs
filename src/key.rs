pub trait IntoKeys {
    type Key<'a>: AsRef<str> + 'a
    where
        Self: 'a;

    fn into_keys<'a>(self) -> impl IntoIterator<Item = Self::Key<'a>>
    where
        Self: 'a;
}

impl IntoKeys for &str {
    type Key<'b>
        = &'b str
    where
        Self: 'b;

    fn into_keys<'b>(self) -> impl IntoIterator<Item = Self::Key<'b>>
    where
        Self: 'b,
    {
        [self]
    }
}

impl IntoKeys for String {
    type Key<'a>
        = String
    where
        Self: 'a;

    fn into_keys<'b>(self) -> impl std::iter::IntoIterator<Item = Self::Key<'b>> {
        [self]
    }
}

impl IntoKeys for &[&str] {
    type Key<'a>
        = &'a str
    where
        Self: 'a;

    fn into_keys<'b>(self) -> impl IntoIterator<Item = Self::Key<'b>>
    where
        Self: 'b,
    {
        self.iter().copied()
    }
}

impl IntoKeys for Vec<String> {
    type Key<'a>
        = String
    where
        Self: 'a;

    fn into_keys<'b>(self) -> impl IntoIterator<Item = Self::Key<'b>> {
        self.into_iter()
    }
}

impl IntoKeys for Vec<&str> {
    type Key<'a>
        = &'a str
    where
        Self: 'a;

    fn into_keys<'b>(self) -> impl IntoIterator<Item = &'b str>
    where
        Self: 'b,
    {
        self.into_iter()
    }
}
