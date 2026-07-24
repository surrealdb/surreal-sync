//! Unified apply events: row changes and relation (graph edge) changes.

use surreal_sync_core::{Change, RelationChange};

/// One item in the apply buffer / transform window / ordered sink queue.
///
/// Sources may interleave row CDC and relation CDC; the apply engine preserves
/// source order across both kinds through the same max_in_flight window.
///
/// [`RelationChange`](Self::RelationChange) is boxed to keep the enum compact
/// (`RelationChange` is substantially larger than [`Change`]).
#[derive(Debug, Clone)]
pub enum ApplyEvent {
    /// Row create / update / delete.
    Change(Change),
    /// Graph-edge create / update / delete ([`RelationChange`]).
    RelationChange(Box<RelationChange>),
}

impl ApplyEvent {
    /// Wrap a row change.
    pub fn change(change: Change) -> Self {
        Self::Change(change)
    }

    /// Wrap a relation change.
    pub fn relation_change(change: RelationChange) -> Self {
        Self::RelationChange(Box::new(change))
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

/// A source event plus the position to advance after sink success.
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
    pub fn change(change: Change, position: P) -> Self {
        Self::new(ApplyEvent::Change(change), position)
    }

    /// Positioned relation change.
    pub fn relation_change(change: RelationChange, position: P) -> Self {
        Self::new(ApplyEvent::relation_change(change), position)
    }
}
