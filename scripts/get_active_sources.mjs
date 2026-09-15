import { Sources } from '../build/sources/index.js';
import dotenv from 'dotenv';
dotenv.config();

console.log(Object.keys(Sources).join(', '));
