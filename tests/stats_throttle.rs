use std::sync::Arc;
use std::time::Duration;

use rocksqueue::stats::StatsCollector;

#[test]
fn test_stats_ema_converges() {
    let collector = StatsCollector::new();
    collector.register("acme");

    // Simulate 10 cycles of consistent traffic
    for _ in 0..10 {
        collector.record_enqueue("acme", 100);
        collector.refresh();
    }

    let stats = collector.snapshot("acme").unwrap();
    // After 10 cycles at 100/cycle, EMA should be close to 100
    assert!(
        stats.arrival_rate > 50.0,
        "EMA should be converging toward 100"
    );
    assert!(!stats.is_new, "Should not be new after enqueues");
}

#[test]
fn test_stats_ack_tracking() {
    let collector = StatsCollector::new();
    collector.register("acme");

    for _ in 0..5 {
        collector.record_ack("acme", Duration::from_millis(200));
    }
    collector.refresh();

    let stats = collector.snapshot("acme").unwrap();
    assert_eq!(stats.total_acked, 5);
}

#[test]
fn test_allocate_slots_proportional() {
    let collector = StatsCollector::new();
    collector.register("heavy");
    collector.register("light");

    // Drive different arrival rates
    for _ in 0..20 {
        collector.record_enqueue("heavy", 100);
        collector.record_enqueue("light", 10);
        collector.refresh();
    }

    // Both should be marked as not-new after activity
    let heavy = collector.snapshot("heavy").unwrap();
    let light = collector.snapshot("light").unwrap();
    assert!(!heavy.is_new);
    assert!(!light.is_new);

    let allocs = collector.allocate_slots(110);
    // heavy should get more slots than light
    let heavy_slots: usize = allocs
        .iter()
        .find(|(id, _)| id == "heavy")
        .map(|(_, s)| *s)
        .unwrap_or(0);
    let light_slots: usize = allocs
        .iter()
        .find(|(id, _)| id == "light")
        .map(|(_, s)| *s)
        .unwrap_or(0);
    assert!(
        heavy_slots >= light_slots,
        "heavy should get >= slots as light"
    );
}

#[test]
fn test_new_tenant_gets_guaranteed_slots() {
    let collector = StatsCollector::new();
    collector.register("brand_new");

    // No enqueue history — still new
    let stats = collector.snapshot("brand_new").unwrap();
    assert!(stats.is_new);

    let allocs = collector.allocate_slots(100);
    let slots = allocs
        .iter()
        .find(|(id, _)| id == "brand_new")
        .map(|(_, s)| *s)
        .unwrap_or(0);
    assert!(
        slots >= collector.min_guarantee_slots,
        "new tenant must get guaranteed slots"
    );
}
