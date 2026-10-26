use super::*;

impl Host {
    /// A single serialized boundary shared by switching and current-session archive.
    pub(super) async fn prepare_replacement(&self, state: &mut HostState) -> Result<()> {
        if state.voice.is_some() || !switching_allowed(&state.manager)? {
            return Err(Error::conflict());
        }
        // Inference may prepare a task immediately before accepting the stop.
        if let Some(runtime) = &state.runtime {
            runtime.handle.end_conversation().await?;
        }
        if !switching_allowed(&state.manager)? {
            return Err(Error::conflict());
        }
        Ok(())
    }

    pub(super) async fn rebuild_sessions(&self, state: &mut HostState) -> Result<()> {
        state.ready = false;
        self.stop_receiver(state).await;
        let stopped = self.stop_runtime(state).await;
        let saved = self.receive_messages(&state.manager).await;
        stopped.and(saved)?;
        let stream = state.manager.delivery_id();
        self.database(move |store| store.finish_delivery(stream))
            .await?;
        let components = self.components.current();
        state.manager = SessionManager::with_agents(
            SessionData::default(),
            components.agents.clone(),
            components.config.session_limits(),
        )?;
        state.selected = None;
        state.capture_sequence = 0;
        Ok(())
    }

    pub(super) fn start_runtime(&self, state: &mut HostState, id: Uuid) -> Result<()> {
        let components = self.components.current();
        let Some(backend) = components.backend.clone() else {
            state.selected = Some(id);
            return Ok(());
        };
        let (handle, mut output) = Core::start(
            components.config.core_config(),
            backend,
            state.manager.clone(),
            id,
        )?;
        let mut events = handle.subscribe();
        let playback = output.playback.clone();
        let mut playback_changes = playback.clone();
        let delivery = self.delivery.clone();
        let sender = self.events.clone();
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let monitor = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => break,
                    changed = playback_changes.changed() => {
                        if changed.is_err() { break; }
                        let current = *playback_changes.borrow_and_update();
                        let channel = delivery.lock().ok().and_then(|d| d.as_ref().map(|(id,_,_)| *id));
                        let _ = sender.send(json!({"kind":"playback","channel_id":channel,"generation":current.generation,"active":current.active}));
                    },
                    event = events.recv() => {
                        let value = match event {
                            Ok(CoreEvent::State(s)) => json!({"kind":"state","session_id":id,"state":format!("{s:?}")}),
                            Ok(CoreEvent::Transcript {user,text,final_chunk}) => json!({"kind":"transcript","session_id":id,"user":user,"text":text,"final_chunk":final_chunk}),
                            Ok(CoreEvent::Execution(_)) => json!({"kind":"sessions_changed"}),
                            Ok(CoreEvent::Error(_)) => {
                                if let Ok(d) = delivery.lock() && let Some((_,_,cancel)) = d.as_ref() {cancel.cancel();}
                                json!({"kind":"error","code":"runtime_failed","message":"capability failed; query current state"})
                            },
                            Err(broadcast::error::RecvError::Lagged(_)) => json!({"kind":"resync"}),
                            Err(broadcast::error::RecvError::Closed) => break,
                            _ => continue,
                        };
                        let _ = sender.send(value);
                    },
                    chunk = output.audio.recv() => {
                        let Some(chunk) = chunk else { break; };
                        if chunk.generation != playback_changes.borrow().generation { continue; }
                        if let Ok(d) = delivery.lock() && let Some((_,tx,cancel)) = d.as_ref() && tx.try_send(chunk).is_err() {cancel.cancel();}
                    },
                }
            }
        });
        state.runtime = Some(Runtime {
            handle,
            playback,
            runner: output.runner,
            monitor,
            cancel,
        });
        state.selected = Some(id);
        Ok(())
    }

    pub(super) async fn reset_backend(&self, runtime: &Runtime) -> Result<()> {
        // Core's explicit end_conversation cancels pending ACP questions. The
        // public unmute control also drops the backend and enters Standby, but
        // preserves those questions, so an audio-only transition uses it.
        runtime.handle.mute(false).await.map_err(Into::into)
    }

    pub(super) async fn active(&self, runtime: &Runtime) -> Result<()> {
        if runtime.handle.state() == CoreState::Active {
            return Ok(());
        }
        let mut events = runtime.handle.subscribe();
        let activate = async {
            runtime.handle.wake(None).await?;
            if runtime.handle.state() == CoreState::Active {
                return Ok(());
            }
            loop {
                match events.recv().await {
                    Ok(CoreEvent::State(CoreState::Active)) => return Ok(()),
                    Ok(CoreEvent::Error(_))
                    | Ok(CoreEvent::State(CoreState::Standby | CoreState::Stopped)) => {
                        return Err(Error::new(
                            503,
                            "activation_failed",
                            "backend activation failed",
                        ));
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(Error::new(
                            503,
                            "activation_failed",
                            "backend activation failed",
                        ));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => match runtime.handle.state() {
                        CoreState::Active => return Ok(()),
                        CoreState::Standby | CoreState::Stopped => {
                            return Err(Error::new(
                                503,
                                "activation_failed",
                                "backend activation failed",
                            ));
                        }
                        _ => (),
                    },
                    _ => (),
                }
            }
        };
        tokio::time::timeout(
            Duration::from_millis(self.config().limits.connect_timeout_ms + 500),
            activate,
        )
        .await
        .map_err(|_| Error::new(504, "activation_timeout", "backend did not become active"))?
    }

    pub(super) async fn stop_runtime(&self, state: &mut HostState) -> Result<()> {
        let Some(runtime) = state.runtime.take() else {
            return Ok(());
        };
        let shutdown =
            tokio::time::timeout(Duration::from_secs(12), runtime.handle.shutdown()).await;
        runtime.cancel.cancel();
        let _ = runtime.monitor.await;
        let mut runner = runtime.runner;
        let joined = tokio::time::timeout(Duration::from_secs(1), &mut runner).await;
        let runner_ok = matches!(joined, Ok(Ok(Ok(()))));
        if joined.is_err() {
            runner.abort();
            let _ = runner.await;
        }
        if !matches!(shutdown, Ok(Ok(()))) || !runner_ok {
            return Err(Error::new(
                504,
                "shutdown_failed",
                "runtime cleanup did not complete",
            ));
        }
        Ok(())
    }
    pub async fn shutdown(&self) -> Result<()> {
        let _update = self.components.update_lock.lock().await;
        if self.stopped.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.lifetime.cancel();
        let mut state = self.state.lock().await;
        let voice = self.close_voice_locked(&mut state, "host_shutdown").await;
        let runtime = self.stop_runtime(&mut state).await;
        let manager = if state.runtime.is_none() {
            state
                .manager
                .shutdown(Duration::from_secs(10))
                .await
                .map_err(Into::into)
        } else {
            Ok(())
        };
        let executors = self.components.current().shutdown().await;
        self.stop_receiver(&mut state).await;
        let saved = self.receive_messages(&state.manager).await;
        voice.and(runtime).and(manager).and(executors).and(saved)?;
        let stream = state.manager.delivery_id();
        self.database(move |store| store.finish_delivery(stream))
            .await
    }
}
