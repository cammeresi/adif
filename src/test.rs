use super::*;

#[test]
fn tag_as_field_returns_some_for_field() {
    let field = Field::new("call", "W1AW");
    let tag = Tag::Field(field);
    assert!(tag.as_field().is_some());
    assert_eq!(tag.as_field().unwrap().name(), "call");
}

#[test]
fn tag_as_field_returns_none_for_eoh() {
    let tag = Tag::Eoh;
    assert!(tag.as_field().is_none());
}

#[test]
fn tag_as_field_returns_none_for_eor() {
    let tag = Tag::Eor;
    assert!(tag.as_field().is_none());
}

#[test]
fn tag_is_eoh_true_for_eoh() {
    let tag = Tag::Eoh;
    assert!(tag.is_eoh());
}

#[test]
fn tag_is_eoh_false_for_eor() {
    let tag = Tag::Eor;
    assert!(!tag.is_eoh());
}

#[test]
fn tag_is_eoh_false_for_field() {
    let field = Field::new("call", "W1AW");
    let tag = Tag::Field(field);
    assert!(!tag.is_eoh());
}

#[test]
fn tag_is_eor_true_for_eor() {
    let tag = Tag::Eor;
    assert!(tag.is_eor());
}

#[test]
fn tag_is_eor_false_for_eoh() {
    let tag = Tag::Eoh;
    assert!(!tag.is_eor());
}

#[test]
fn tag_is_eor_false_for_field() {
    let field = Field::new("call", "W1AW");
    let tag = Tag::Field(field);
    assert!(!tag.is_eor());
}
