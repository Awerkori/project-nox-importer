import fs from 'fs';
import path from 'path';
import archiver from 'archiver';

const output = fs.createWriteStream('dist-discloud/project-nox-importer.zip');
const archive = archiver('zip', { zlib: { level: 9 } });

output.on('close', function() {
  console.log(archive.pointer() + ' total bytes');
  console.log('archiver has been finalized and the output file descriptor has closed.');
});

archive.pipe(output);

// Add directories
archive.directory('dist/', 'dist');
// Add files
archive.file('package.json', { name: 'package.json' });
archive.file('discloud.config', { name: 'discloud.config' });
// Add package-lock.json if needed
if (fs.existsSync('package-lock.json')) {
  archive.file('package-lock.json', { name: 'package-lock.json' });
}

archive.finalize();
