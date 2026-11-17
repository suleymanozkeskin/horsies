//! Workflows: the typed DSL, workflow definitions, the conversion to the
//! core's spec data, the start forms and the workflow handle.

use std::marker::PhantomData;

/// The typed token of one field of a workflow node's input type.
///
/// `#[derive(WorkflowInput)]` makes one per field of a named-field input
/// struct, and `#[task]` makes one per parameter in the task's `params`
/// module. It carries the input type and the field type; the node keeps
/// only the field's JSON key.
#[derive(Debug, PartialEq, Eq)]
pub struct InputField<I, F> {
    name: &'static str,
    _types: PhantomData<fn() -> (I, F)>,
}

impl<I, F> InputField<I, F> {
    /// The token of the field whose JSON key is `name`. The macro
    /// expansions make it through `__private::input_field`.
    pub(crate) const fn raw(name: &'static str) -> Self {
        Self {
            name,
            _types: PhantomData,
        }
    }

    /// The JSON key of the field in the node's input.
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

impl<I, F> Clone for InputField<I, F> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<I, F> Copy for InputField<I, F> {}

#[cfg(test)]
mod tests {
    use super::InputField;

    struct Input;

    #[test]
    fn a_token_gives_the_key_it_was_made_with_and_its_copy_gives_the_same() {
        let token: InputField<Input, u32> = crate::__private::input_field("user_id");
        let copy = token;
        assert_eq!(token.name(), "user_id");
        assert_eq!(copy.name(), "user_id");
    }
}
