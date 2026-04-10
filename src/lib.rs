/*!
# cuda-schedule

Time-based task scheduling.

Agents live in time. Tasks have deadlines, meetings have windows,
maintenance has intervals. This crate gives agents a sense of time
and the ability to manage it.

- Tasks with priority and deadlines
- Time windows and availability
- Recurring tasks
- Conflict detection and resolution
- Schedule optimization
*/

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A scheduled task
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub name: String,
    pub priority: TaskPriority,
    pub effort: f64,
    pub deadline_ms: Option<u64>,
    pub start_ms: u64,
    pub duration_ms: u64,
    pub status: TaskStatus,
    pub category: String,
    pub recurring: Option<Recurring>,
    pub dependencies: Vec<String>,
    pub completed_at: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TaskPriority { Low, Normal, High, Critical }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus { Pending, Scheduled, Running, Completed, Cancelled, Overdue }

/// Recurring task config
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Recurring {
    pub interval_ms: u64,
    pub max_occurrences: Option<u32>,
    pub occurrences_done: u32,
}

/// A time window
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimeWindow {
    pub start_ms: u64,
    pub end_ms: u64,
    pub label: String,
}

/// Schedule conflict
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Conflict {
    pub task_a: String,
    pub task_b: String,
    pub overlap_ms: u64,
    pub resolution: ConflictResolution,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictResolution { DeferB, DeferA, Parallelize, Split }

/// The scheduler
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Scheduler {
    pub tasks: HashMap<String, Task>,
    pub windows: Vec<TimeWindow>,
    pub conflicts: Vec<Conflict>,
    pub next_task_id: u64,
}

impl Scheduler {
    pub fn new() -> Self { Scheduler { tasks: HashMap::new(), windows: vec![], conflicts: vec![], next_task_id: 1 } }

    /// Add a task
    pub fn add(&mut self, task: Task) {
        let id = task.id.clone();
        self.tasks.insert(id, task);
    }

    /// Create a simple task
    pub fn make_task(name: &str, priority: TaskPriority, effort: f64, duration_ms: u64) -> Task {
        let id = format!("task_{}", self.next_task_id);
        self.next_task_id += 1;
        Task { id: id.clone(), name: name.to_string(), priority, effort, deadline_ms: None, start_ms: now(), duration_ms, status: TaskStatus::Pending, category: String::new(), recurring: None, dependencies: vec![], completed_at: None }
    }

    /// Schedule a task at a specific time
    pub fn schedule_at(&mut self, task_id: &str, start_ms: u64) {
        if let Some(task) = self.tasks.get_mut(task_id) {
            task.start_ms = start_ms;
            task.status = TaskStatus::Scheduled;
        }
    }

    /// Mark task complete
    pub fn complete(&mut self, task_id: &str) {
        if let Some(task) = self.tasks.get_mut(task_id) {
            task.status = TaskStatus::Completed;
            task.completed_at = Some(now());
            // Handle recurring
            if let Some(ref mut rec) = task.recurring {
                rec.occurrences_done += 1;
                if rec.max_occurrences.map_or(true, |max| rec.occurrences_done < max) {
                    let next_start = task.start_ms + rec.interval_ms;
                    task.status = TaskStatus::Pending;
                    task.start_ms = next_start;
                    task.completed_at = None;
                }
            }
        }
    }

    /// Get next task to execute (highest priority, closest deadline)
    pub fn next_task(&self) -> Option<&Task> {
        self.tasks.values()
            .filter(|t| t.status == TaskStatus::Pending || t.status == TaskStatus::Scheduled)
            .min_by(|a, b| {
                // Sort by priority (Critical first), then deadline, then effort
                match b.priority.cmp(&a.priority) {
                    std::cmp::Ordering::Equal => {
                        let deadline_a = a.deadline_ms.unwrap_or(u64::MAX);
                        let deadline_b = b.deadline_ms.unwrap_or(u64::MAX);
                        match deadline_a.cmp(&deadline_b) {
                            std::cmp::Ordering::Equal => a.effort.partial_cmp(&b.effort).unwrap_or(std::cmp::Ordering::Equal),
                            other => other,
                        }
                    }
                    other => other,
                }
            })
    }

    /// Detect conflicts (overlapping scheduled tasks)
    pub fn detect_conflicts(&mut self) {
        self.conflicts.clear();
        let scheduled: Vec<&Task> = self.tasks.values()
            .filter(|t| t.status == TaskStatus::Scheduled)
            .collect();

        for i in 0..scheduled.len() {
            for j in (i+1)..scheduled.len() {
                let a = scheduled[i];
                let b = scheduled[j];
                let a_end = a.start_ms + a.duration_ms;
                let b_end = b.start_ms + b.duration_ms;
                if a.start_ms < b_end && b.start_ms < a_end {
                    let overlap_start = a.start_ms.max(b.start_ms);
                    let overlap_end = a_end.min(b_end);
                    let overlap = overlap_end - overlap_start;
                    // Defer lower priority
                    let resolution = if a.priority > b.priority { ConflictResolution::DeferB }
                        else if b.priority > a.priority { ConflictResolution::DeferA }
                        else { ConflictResolution::Split };
                    self.conflicts.push(Conflict { task_a: a.id.clone(), task_b: b.id.clone(), overlap_ms: overlap, resolution });
                }
            }
        }
    }

    /// Get overdue tasks
    pub fn overdue_tasks(&self) -> Vec<&Task> {
        let now = now();
        self.tasks.values()
            .filter(|t| {
                t.deadline_ms.map_or(false, |d| d < now) &&
                t.status != TaskStatus::Completed &&
                t.status != TaskStatus::Cancelled
            })
            .collect()
    }

    /// Check if a time window is available
    pub fn is_available(&self, start_ms: u64, duration_ms: u64) -> bool {
        let end_ms = start_ms + duration_ms;
        !self.windows.iter().any(|w| start_ms < w.end_ms && end_ms > w.start_ms)
    }

    /// Urgency score for a task (0-1, higher = more urgent)
    pub fn urgency(&self, task: &Task) -> f64 {
        let priority_score = match task.priority {
            TaskPriority::Critical => 1.0,
            TaskPriority::High => 0.8,
            TaskPriority::Normal => 0.5,
            TaskPriority::Low => 0.2,
        };
        let deadline_score = match task.deadline_ms {
            Some(deadline) => {
                let remaining = (deadline as i64 - now() as i64).max(0) as f64;
                let window = 3600_000.0; // 1 hour
                if remaining < window { 1.0 } else { (window / remaining).min(1.0) }
            }
            None => 0.0,
        };
        (priority_score * 0.6 + deadline_score * 0.4).min(1.0)
    }

    /// Summary
    pub fn summary(&self) -> String {
        let total = self.tasks.len();
        let pending = self.tasks.values().filter(|t| t.status == TaskStatus::Pending).count();
        let completed = self.tasks.values().filter(|t| t.status == TaskStatus::Completed).count();
        let overdue = self.overdue_tasks().len();
        format!("Scheduler: {} tasks ({} pending, {} completed, {} overdue), {} conflicts",
            total, pending, completed, overdue, self.conflicts.len())
    }
}

fn now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_task() {
        let mut sch = Scheduler::new();
        let task = sch.make_task("patrol", TaskPriority::Normal, 0.5, 5000);
        sch.add(task);
        assert_eq!(sch.tasks.len(), 1);
    }

    #[test]
    fn test_next_task_priority() {
        let mut sch = Scheduler::new();
        sch.add(sch.make_task("low", TaskPriority::Low, 0.1, 1000));
        sch.add(sch.make_task("critical", TaskPriority::Critical, 0.9, 1000));
        let next = sch.next_task().unwrap();
        assert_eq!(next.priority, TaskPriority::Critical);
    }

    #[test]
    fn test_complete_task() {
        let mut sch = Scheduler::new();
        sch.add(sch.make_task("x", TaskPriority::Normal, 0.5, 1000));
        sch.complete("task_1");
        assert_eq!(sch.tasks["task_1"].status, TaskStatus::Completed);
    }

    #[test]
    fn test_recurring_task() {
        let mut sch = Scheduler::new();
        let mut task = sch.make_task("health_check", TaskPriority::Normal, 0.1, 1000);
        task.recurring = Some(Recurring { interval_ms: 60_000, max_occurrences: None, occurrences_done: 0 });
        task.start_ms = now();
        sch.add(task);
        sch.complete("task_1");
        assert_eq!(sch.tasks["task_1"].status, TaskStatus::Pending); // rescheduled
        assert_eq!(sch.tasks["task_1"].recurring.as_ref().unwrap().occurrences_done, 1);
    }

    #[test]
    fn test_detect_conflicts() {
        let mut sch = Scheduler::new();
        let mut t1 = sch.make_task("a", TaskPriority::Normal, 0.5, 5000);
        t1.start_ms = 1000; t1.status = TaskStatus::Scheduled;
        let mut t2 = sch.make_task("b", TaskPriority::Normal, 0.5, 5000);
        t2.start_ms = 3000; t2.status = TaskStatus::Scheduled; // overlaps with a
        sch.add(t1); sch.add(t2);
        sch.detect_conflicts();
        assert_eq!(sch.conflicts.len(), 1);
    }

    #[test]
    fn test_overdue() {
        let mut sch = Scheduler::new();
        let mut task = sch.make_task("late", TaskPriority::High, 0.5, 1000);
        task.deadline_ms = Some(0); // past deadline
        sch.add(task);
        assert_eq!(sch.overdue_tasks().len(), 1);
    }

    #[test]
    fn test_urgency() {
        let mut sch = Scheduler::new();
        let mut task = sch.make_task("x", TaskPriority::Critical, 0.5, 1000);
        task.deadline_ms = Some(now() + 100);
        sch.add(task);
        let u = sch.urgency(sch.tasks.get("task_1").unwrap());
        assert!(u > 0.8);
    }

    #[test]
    fn test_time_window_available() {
        let mut sch = Scheduler::new();
        assert!(sch.is_available(1000, 5000)); // no windows = always available
        sch.windows.push(TimeWindow { start_ms: 2000, end_ms: 8000, label: "maintenance".into() });
        assert!(!sch.is_available(3000, 2000)); // overlaps window
        assert!(sch.is_available(1000, 500)); // before window
    }

    #[test]
    fn test_recurring_max() {
        let mut sch = Scheduler::new();
        let mut task = sch.make_task("x", TaskPriority::Normal, 0.1, 100);
        task.recurring = Some(Recurring { interval_ms: 1000, max_occurrences: Some(2), occurrences_done: 1 });
        sch.add(task);
        sch.complete("task_1");
        // After completing 2nd occurrence, should be completed (max reached)
        assert_eq!(sch.tasks["task_1"].status, TaskStatus::Completed);
    }

    #[test]
    fn test_summary() {
        let sch = Scheduler::new();
        let s = sch.summary();
        assert!(s.contains("0 tasks"));
    }
}
