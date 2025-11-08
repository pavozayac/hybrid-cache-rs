use serde::Serialize;

pub trait AsEntries<T: Serialize> {
    fn as_entries<'a>(&'a self) -> impl IntoIterator<Item = (&'a str, &'a T)>
    where
        T: 'a;
}

impl<T: Serialize> AsEntries<T> for (&str, T) {
    fn as_entries<'a>(&'a self) -> impl IntoIterator<Item = (&'a str, &'a T)>
    where
        T: 'a,
    {
        [(self.0, &self.1)]
    }
}

impl<T: Serialize> AsEntries<T> for (String, T) {
    fn as_entries<'a>(&'a self) -> impl IntoIterator<Item = (&'a str, &'a T)>
    where
        T: 'a,
    {
        [((&*self.0), &self.1)]
    }
}

impl<T: Serialize> AsEntries<T> for &[(String, T)] {
    fn as_entries<'a>(&'a self) -> impl IntoIterator<Item = (&'a str, &'a T)>
    where
        T: 'a,
    {
        self.iter().map(|(k, v)| ((&**k), v))
    }
}

impl<T: Serialize> AsEntries<T> for Vec<(String, T)> {
    fn as_entries<'a>(&'a self) -> impl IntoIterator<Item = (&'a str, &'a T)>
    where
        T: 'a,
    {
        self.iter().map(|(k, v)| ((&**k), v))
    }
}

impl<T: Serialize> AsEntries<T> for &[(&str, T)] {
    fn as_entries<'a>(&'a self) -> impl IntoIterator<Item = (&'a str, &'a T)>
    where
        T: 'a,
    {
        self.iter().map(|(k, v)| (*k, v))
    }
}

impl<T: Serialize> AsEntries<T> for Vec<(&str, T)> {
    fn as_entries<'a>(&'a self) -> impl IntoIterator<Item = (&'a str, &'a T)>
    where
        T: 'a,
    {
        self.iter().map(|(k, v)| (*k, v))
    }
}

impl<T: Serialize> AsEntries<T> for std::collections::HashMap<String, T> {
    fn as_entries<'a>(&'a self) -> impl IntoIterator<Item = (&'a str, &'a T)>
    where
        T: 'a,
    {
        self.iter().map(|(k, v)| (k.as_str(), v))
    }
}

impl<T: Serialize> AsEntries<T> for std::collections::HashMap<&str, T> {
    fn as_entries<'a>(&'a self) -> impl IntoIterator<Item = (&'a str, &'a T)>
    where
        T: 'a,
    {
        self.iter().map(|(k, v)| (*k, v))
    }
}
