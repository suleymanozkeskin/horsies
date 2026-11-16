//! Process-wide registry for workflow handles.
//!
//! Populated automatically during `register_workflow_definition` and
//! `workflow_template` calls. Enables `start_workflow::<D>()` and
//! `start_workflow_with::<D>(params)` from any call site.

use std::any::{type_name, Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

type AnyHandle = Arc<dyn Any + Send + Sync>;
type HandleMap = RwLock<HashMap<TypeId, AnyHandle>>;

static REGISTRY: OnceLock<HandleMap> = OnceLock::new();

fn registry() -> &'static HandleMap {
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Store a workflow handle keyed by the definition type `D`.
pub(crate) fn store_workflow<D: 'static, H: Send + Sync + 'static>(handle: H) {
    let mut map = registry()
        .write()
        .expect("global workflow registry poisoned");
    map.insert(TypeId::of::<D>(), Arc::new(handle));
}

/// Retrieve a workflow handle by definition type `D`, downcast to `H`.
pub(crate) fn get_workflow<D: 'static, H: Clone + Send + Sync + 'static>() -> Option<H> {
    let map = registry()
        .read()
        .expect("global workflow registry poisoned");
    let arc = map.get(&TypeId::of::<D>())?;
    let typed = arc.clone().downcast::<H>().ok()?;
    Some((*typed).clone())
}

/// Human-readable type name for error messages.
pub(crate) fn definition_type_name<D: 'static>() -> &'static str {
    type_name::<D>()
}
