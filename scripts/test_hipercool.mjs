import { Sources } from '../build/sources/index.js';
async function test() {
  const source = Sources.find(s => s.id === 'hipercool');
  console.log(await source.getWorkManifest('https://lerhentais.com/series/sakamoto-days'));
}
test().catch(console.error);
