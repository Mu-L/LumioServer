from pathlib import Path
import re
import subprocess

MAIN='8c3883d42759b384742c6017b358ad824cb2f1e6'

def git(*args, check=True):
    return subprocess.run(['git',*args],text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,check=check)

def fn(text, marker):
    a=text.index(marker);b=text.index('{',a)+1;depth=1
    while depth:
        if text[b]=='{':depth+=1
        elif text[b]=='}':depth-=1
        b+=1
    return text[a:b]

git('fetch','origin','main')
if git('merge-base','--is-ancestor',MAIN,'HEAD',check=False).returncode:
    result=git('merge','--no-commit','--no-ff',MAIN,check=False)
    print(result.stdout)
    conflicted=git('diff','--name-only','--diff-filter=U').stdout.splitlines()
    print('Conflicted paths:',conflicted)
    allowed={'modules/process/src/entity_chat/suite.rs','modules/process/src/entity_chat/host.rs','modules/process/src/entity_chat/mod.rs','modules/process/tests/entity_chat_host.rs'}
    if set(conflicted)-allowed:
        raise RuntimeError('unreviewed merge conflict; refusing automatic resolution')
    for name in conflicted:
        if name=='modules/process/tests/entity_chat_host.rs':
            ours=git('show',f':2:{name}').stdout
            theirs=git('show',f':3:{name}').stdout
            updated=fn(ours,'fn host_wire_ingress_waits_for_a_clock_tick_at_the_batch_limit()')
            old=fn(theirs,'fn host_wire_ingress_ticks_at_max_chat_inputs()')
            Path(name).write_text(theirs.replace(old,updated))
        else:
            # Only competing evidence fallbacks may pick the stricter PR38 arm.
            # Non-overlapping upstream edits (real wait/S9/clock observer) remain.
            text=Path(name).read_text()
            pattern=re.compile(r'^<<<<<<< .*?\n(.*?)^=======\n(.*?)^>>>>>>> .*?\n',re.M|re.S)
            def resolve(match):
                left,right=match.group(1),match.group(2)
                print('Resolving reviewed conflict:',name,'\nOURS:\n',left,'\nUPSTREAM:\n',right)
                if name.endswith('suite.rs') and 'wrongPasswordCode' in (left+right):
                    return left
                raise RuntimeError('conflict requires manual reconciliation; no source discarded')
            text=pattern.sub(resolve,text)
            if '<<<<<<< ' in text: raise RuntimeError('unresolved merge markers')
            Path(name).write_text(text)
        git('add',name)
# Newly added deterministic tests must use the new explicitly-test-only method.
for name in ['modules/process/src/entity_chat/host_hardening_tests.rs','modules/process/src/entity_chat/secure.rs']:
    p=Path(name);text=p.read_text()
    text=re.sub(r'(?m)^(\s*)([A-Za-z_][\w.()]*)\.advance_ms\(([^;\n]*)\);',r'\1assert!(\2.advance_test_clock(\3));',text)
    p.write_text(text)
Path(__file__).unlink()
print('Preserved upstream real-clock immutability and S9 real-time evidence semantics.')
