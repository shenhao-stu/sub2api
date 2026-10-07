import fcntl,os,pathlib,shutil,subprocess,tempfile,unittest
SOURCE=pathlib.Path(os.environ.get('BACKUP_SCRIPT',str(pathlib.Path(__file__).resolve().parents[1]/'getoken-daily-backup.sh')))
@unittest.skipUnless(shutil.which('flock'), 'requires Linux flock')
class BackupSafety(unittest.TestCase):
 def exercise(self,fs='ext4',locked=False,fail=False):
  with tempfile.TemporaryDirectory()as d:
   root=pathlib.Path(d);stage=root/'staging';stage.mkdir();bin=root/'bin';bin.mkdir();events=root/'events';body=SOURCE.read_text()
   body=body.replace('/opt/getoken-consolidated/backups/borg-repo',str(root/'repo')).replace('/opt/getoken-consolidated/backups/staging',str(stage)).replace('/var/log/getoken-backup.log',str(root/'log')).replace('/opt/getoken-consolidated/getoken-panel/data/.panel.bak.db',str(root/'panel.db'))
   (root/'panel.db').write_text('fixture')
   mocks={'findmnt':f'echo {fs}', 'docker':f'echo docker >> "{events}"; cat >/dev/null; echo dump', 'borg':f'echo borg >> "{events}"; '+('exit 2'if fail else 'exit 0')}
   for name,code in mocks.items():p=bin/name;p.write_text('#!/bin/sh\n'+code+'\n');p.chmod(0o755)
   script=root/'run.sh';script.write_text(body)
   lock=(stage/'.backup.lock').open('a')
   if locked:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
   r=subprocess.run(['bash',str(script)],capture_output=True,text=True,input='',env={**os.environ,'PATH':str(bin)+':'+os.environ['PATH']})
   event=events.read_text()if events.exists()else ''
   self.assertFalse(list(stage.glob('gk-bk.*')))
   self.assertEqual(stage.stat().st_mode&0o777,0o700)
   lock.close();return r,event
 def test_disk_success(self):
  r,events=self.exercise();self.assertEqual(r.returncode,0,r.stderr);self.assertEqual(events.count('docker'),3);self.assertEqual(events.count('borg'),4)
 def test_tmpfs_fail_before_database(self):
  r,events=self.exercise('tmpfs');self.assertNotEqual(r.returncode,0);self.assertEqual(events,'')
 def test_ramfs_fail_before_database(self):
  r,events=self.exercise('ramfs');self.assertNotEqual(r.returncode,0);self.assertEqual(events,'')
 def test_overlapping_backup_fails_before_database(self):
  r,events=self.exercise(locked=True);self.assertNotEqual(r.returncode,0);self.assertEqual(events,'')
 def test_failure_cleans_only_owned_stage(self):
  r,events=self.exercise(fail=True);self.assertNotEqual(r.returncode,0);self.assertEqual(events.count('borg'),1)
if __name__=='__main__':unittest.main()
