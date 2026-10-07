import fs from 'fs';
const file = 'src/core/gap-validator.ts';
let code = fs.readFileSync(file, 'utf8');

const qCheckRegex = /\/\/ 3\. Check if any job exists in importer_queue[\s\S]*?\} catch \(err: any\) \{[\s\S]*?sourcesChecked,\n\s+\};\n\s+\}/;
const match = code.match(qCheckRegex);
if (match) {
  const qCheckCode = match[0];
  code = code.replace(qCheckRegex, '');
  
  // Insert it after `let sourcesChecked: string[] = [primarySource];`
  code = code.replace(
    /let sourcesChecked: string\[\] = \[primarySource\];\n/,
    `let sourcesChecked: string[] = [primarySource];\n\n  ` + qCheckCode.replace('// 3.', '// 1.') + `\n`
  );
  
  fs.writeFileSync(file, code);
  console.log('Fixed gap-validator.ts');
} else {
  console.log('Regex failed');
}
