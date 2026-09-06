from pathlib import Path
p = Path('modules/process/src/entity_chat/wire.rs')
s = p.read_text()
s = s.replace('write_frames(&mut sink, &pending, &cancel, &wake)', 'write_frames(&mut sink, pending, &cancel, &wake)')
s = s.replace('pending: &lumio_host_runtime::Receiver<WireOut>', 'pending: lumio_host_runtime::Receiver<WireOut>')
s = s.replace('notified.as_mut().enable();', 'let _ = notified.as_mut().enable();')
p.write_text(s)
# A receiver borrowed across await would require Sync. Ownership above is
# intentionally moved into the socket task: std MPSC Receiver is Send, not Sync.
p = Path('modules/process/src/entity_chat/host.rs'); s = p.read_text()
s = s.replace('''                            panic!("owner forwarding deadline exceeded or owner closed");''', '''                            if cancel.is_cancelled() { break; }
                            panic!("owner forwarding deadline exceeded or owner closed");''')
p.write_text(s)
Path(__file__).unlink()
