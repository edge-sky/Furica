use super::*;

fn run(trigger: &mut Trigger, probabilities: &[f32]) -> Vec<VadEvent> {
    probabilities
        .iter()
        .filter_map(|&p| trigger.process(p).unwrap())
        .collect()
}

#[test]
fn rejects_short_noise_and_backdates_confirmed_onset() {
    let mut trigger = Trigger::new(VadConfig::default()).unwrap();
    assert_eq!(
        run(&mut trigger, &[0.9, 0.1, 0.5, 0.5, 0.9]),
        vec![VadEvent::SpeechStarted { start_sample: 1024 }]
    );
    assert_eq!(
        trigger.finish(),
        Some(VadEvent::SpeechEnded { end_sample: 2560 })
    );
}

#[test]
fn silence_must_be_consecutive_and_rounds_up_to_complete_frames() {
    let mut trigger = Trigger::new(VadConfig::default()).unwrap();
    run(&mut trigger, &[0.8, 0.8]);
    assert!(run(&mut trigger, &[0.1; 9]).is_empty());
    // Exactly the negative threshold breaks a pending silence run.
    assert!(run(&mut trigger, &[0.35]).is_empty());
    assert!(run(&mut trigger, &[0.1; 9]).is_empty());
    assert_eq!(
        run(&mut trigger, &[0.1]),
        vec![VadEvent::SpeechEnded {
            end_sample: 12 * 512
        }]
    );
    assert_eq!(trigger.finish(), None);
    assert!(run(&mut trigger, &[0.1; 10]).is_empty());
    assert_eq!(
        run(&mut trigger, &[0.5, 0.5]),
        vec![VadEvent::SpeechStarted {
            start_sample: 32 * 512
        }]
    );
}

#[test]
fn hysteresis_keeps_speech_active_but_cannot_start_it() {
    let mut trigger = Trigger::new(VadConfig::default()).unwrap();
    assert!(run(&mut trigger, &[0.4; 10]).is_empty());
    assert!(run(&mut trigger, &[0.8, 0.4, 0.8]).is_empty());
    assert_eq!(
        run(&mut trigger, &[0.8]),
        vec![VadEvent::SpeechStarted {
            start_sample: 12 * 512
        }]
    );
    assert!(run(&mut trigger, &[0.4; 20]).is_empty());
    assert_eq!(
        trigger.finish(),
        Some(VadEvent::SpeechEnded {
            end_sample: 34 * 512
        })
    );
}

#[test]
fn finish_excludes_pending_silence_and_reset_discards_candidates() {
    let mut trigger = Trigger::new(VadConfig::default()).unwrap();
    run(&mut trigger, &[0.8]);
    assert_eq!(trigger.finish(), None);
    trigger.reset();
    assert_eq!(
        run(&mut trigger, &[0.8, 0.8, 0.1]),
        vec![VadEvent::SpeechStarted { start_sample: 0 }]
    );
    assert_eq!(
        trigger.finish(),
        Some(VadEvent::SpeechEnded { end_sample: 1024 })
    );
    trigger.reset();
    assert_eq!(trigger.finish(), None);
    assert!(run(&mut trigger, &[0.8]).is_empty());
    assert_eq!(
        run(&mut trigger, &[0.8]),
        vec![VadEvent::SpeechStarted { start_sample: 0 }]
    );
}

#[test]
fn positive_sub_frame_durations_still_require_a_frame() {
    let mut trigger = Trigger::new(VadConfig {
        min_speech: Duration::from_nanos(1),
        min_silence: Duration::from_nanos(1),
        ..VadConfig::default()
    })
    .unwrap();
    assert_eq!(
        run(&mut trigger, &[0.5, 0.0, 0.5, 0.0]),
        vec![
            VadEvent::SpeechStarted { start_sample: 0 },
            VadEvent::SpeechEnded { end_sample: 512 },
            VadEvent::SpeechStarted { start_sample: 1024 },
            VadEvent::SpeechEnded { end_sample: 1536 },
        ]
    );
}

#[test]
fn invalid_probabilities_and_position_overflow_do_not_advance_trigger() {
    let mut trigger = Trigger::new(VadConfig::default()).unwrap();
    for p in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -0.1, 1.1] {
        assert!(matches!(
            trigger.process(p),
            Err(VadError::InvalidOutput(_))
        ));
    }
    assert_eq!(
        run(&mut trigger, &[1.0, 1.0]),
        vec![VadEvent::SpeechStarted { start_sample: 0 }]
    );
    trigger.position = u64::MAX - 511;
    let position = trigger.position;
    assert!(matches!(
        trigger.process(0.0),
        Err(VadError::PositionOverflow)
    ));
    assert_eq!(trigger.position, position);
}

#[test]
fn rejects_invalid_configuration_without_truncating_durations() {
    for (speech, silence) in [
        (f32::NAN, 0.35),
        (0.5, f32::NAN),
        (f32::INFINITY, 0.35),
        (0.5, f32::NEG_INFINITY),
        (1.0, 0.35),
        (0.5, 0.0),
        (0.5, 0.5),
        (0.3, 0.4),
        (-0.1, -0.2),
    ] {
        assert!(matches!(
            Trigger::new(VadConfig {
                speech_threshold: speech,
                silence_threshold: silence,
                ..VadConfig::default()
            }),
            Err(VadError::Config(_))
        ));
    }
    for duration in [Duration::ZERO, Duration::MAX] {
        for speech in [true, false] {
            let mut config = VadConfig::default();
            if speech {
                config.min_speech = duration;
            } else {
                config.min_silence = duration;
            }
            assert!(matches!(Trigger::new(config), Err(VadError::Config(_))));
        }
    }
    assert_eq!(duration_samples(Duration::from_nanos(62_501)).unwrap(), 2);
    assert_eq!(duration_samples(Duration::from_millis(300)).unwrap(), 4800);
}
