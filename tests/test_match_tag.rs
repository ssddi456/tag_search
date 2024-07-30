extern crate search;

use search::{match_tag, match_tags, padding_tag}; // Import the match_tags function


#[test]
fn test_match_tags() {
    
    assert_eq!(match_tag(String::from("a"), String::from("a b c")), true);
    assert_eq!(match_tag(String::from("b"), String::from("a b c")), true);
    assert_eq!(match_tag(String::from("c"), String::from("a b c")), true);
    assert_eq!(match_tag(String::from("e"), String::from("a be c")), false);
    assert_eq!(match_tag(String::from("e"), String::from("a be c e")), true);
    assert_eq!(match_tag(String::from("d"), String::from("a b c")), false);

    assert_eq!(match_tags(&vec![b"a".to_vec(), b"b".to_vec()], &b"1 a b c ".to_vec()), true);
    assert_eq!(match_tags(&vec![b"a".to_vec(), b"b".to_vec()], &b"1 a b ".to_vec()), true);
    assert_eq!(match_tags(&vec![b"a".to_vec(), b"e".to_vec()], &b"1 a b ".to_vec()), false);
    assert_eq!(match_tags(&vec![b"a".to_vec(), b"e".to_vec()], &b"1 a b c d e ".to_vec()), true);
    assert_eq!(match_tags(&vec![b"a".to_vec(), b"e".to_vec()], &b"1 a be c d e f ".to_vec()), true);
    assert_eq!(match_tags(&vec![b"a".to_vec(), b"ef".to_vec()], &b"1 a be c d ef ".to_vec()), true);
}

#[test]
fn test_text_match() {

    assert_eq!(padding_tag(&String::from('a')), vec![b' ', b'a', b' ']);

    let tags1 = vec![
        padding_tag(&String::from('a')),
        padding_tag(&String::from('b')),
    ];
    assert_eq!(match_tags(&tags1, &b"1 a b c ".to_vec()), true);

    let tags2 = vec![
        padding_tag(&String::from('a')),
        padding_tag(&String::from('c')),
    ];
    assert_eq!(match_tags(&tags2, &b"1 a b c ".to_vec()), true);
    let tags3 = vec![
        padding_tag(&String::from("ab")),
        padding_tag(&String::from("cc")),
    ];
    assert_eq!(match_tags(&tags3, &b"1 ab b cc ".to_vec()), true);

    let str1 = "1 abc ccc ";
    let vec1 = str1.as_bytes().to_vec();

    let tags4 = vec![
        padding_tag(&String::from("abc")),
        padding_tag(&String::from("ccc")),
    ];

    assert_eq!(match_tags(&tags4, &vec1), true);

}
