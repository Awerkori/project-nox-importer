import fs from 'fs';
const file = 'src/core/scheduler/admission-controller.ts';
let code = fs.readFileSync(file, 'utf8');

const target = \`          const minSort = cand.min_sort_key ? parseFloat(cand.min_sort_key) : 0;
          const gapStart = maxPub >= 0 ? maxPub + 1 : 1;
          const gapEnd = minSort - 1;
          if (gapStart > gapEnd) continue;\`;

const replacement = \`          const minSort = cand.min_sort_key ? parseFloat(cand.min_sort_key) : 0;
          const gapStart = maxPub >= 0 ? maxPub + 1 : 1;
          const gapEnd = minSort - 1;
          if (gapStart > gapEnd) {
            return cand;
          }\`;

const newCode = code.replace(target, replacement);
if (newCode === code) {
  console.log('No match found!');
} else {
  fs.writeFileSync(file, newCode);
  console.log('Replaced findOnDemandFrontier gap check');
}
