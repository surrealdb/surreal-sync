//! Unified apply events: row changes and relation (graph edge) changes.

use sync_core::{UniversalChange, UniversalRelationChange};

/// One item in the apply buffer / transform window / ordered sink queue.
///
/// Sources may interleave row CDC and relation CDC; the apply engine preserves
/// source order across both kinds through the same max_in_flight window.
#[derive(Debug, Clone)]
pub enum ApplyEvent {
    /// Row create / update / delete.
    Change(UniversalChange),
    /// Graph-edge create / update / delete ([`UniversalRelationChange`]).
    RelationChange(UniversalRelationChange),
}

impl ApplyEvent {
    /// Wrap a row change.
    pub fn change(change: UniversalChange) -> Self {
        Self::Change(change)
    }

    /// Wrap a relation change.
    pub fn relation_change(change: UniversalRelationChange) -> Self {
        Self::RelationChange(change)
    }

    /// Whether this is a row change.
    pub fn is_change(&self) -> bool {
        matches!(self, Self::Change(_))
    }

    /// Whether this is a relation change.
    pub fn is_relation_change(&self) -> bool {
        matches!(self, Self::RelationChange(_))
    }
}

/// A source event plus the position to commit after sink success.
#[derive(Debug, Clone)]
pub struct PositionedEvent<P> {
    /// Event to transform and apply.
    pub event: ApplyEvent,
    /// Source position associated with this event (checkpoint candidate).
    pub position: P,
}

impl<P> PositionedEvent<P> {
    /// Construct a positioned event.
    pub fn new(event: ApplyEvent, position: P) -> Self {
        Self { event, position }
    }

    /// Positioned row change.
    pub fn change(change: UniversalChange, position: P) -> Self {
        Self::new(ApplyEvent::Change(change), position)
    }

    /// Positioned relation change.
    pub fn relation_change(change: UniversalRelationChange, position: P) -> Self {
        Self::new(ApplyEvent::RelationChange(change), position)
    }
}
