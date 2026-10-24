import { execFile } from 'child_process';
import { promisify } from 'util';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import { isPtBrTitle, cleanPtBrCandidateTitle, isConsecratedProperTitle, isPortugueseWord } from './ptbr-title-localization.js';
import { alternativeTitleKey } from './alternative-titles.js';
const execFileAsync = promisify(execFile);
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const TESSDATA_DIR = path.join(__dirname, 'tessdata');
const SCANLATOR_NOISE_REGEX = /(?:traduzido\s+por|tradu[çc][aã]o|edi[çc][aã]o|revis[aã]o|raw|cleaner|typesetter|qc|leia\s+em|visite|dispon[íi]vel\s+em|scan|scanlator|scans|fansub|projeto|equipe|discord|site|\.com|\.br|\.net|\.org|instagram|twitter|apoie|doar|chave\s+pix|pix|patreon|catarse|manga\s*plus|leitor|leiamangas|leronline|direitos\s+reservados)/i;
const CHAPTER_VOLUME_NOISE_REGEX = /^(?:vol\.?\s*\d+|cap\.?\s*\d+|cap[íi]tulo\s*\d+|epis[óo]dio\s*\d+|parte\s*\d+|\+18|\+16|livre|adulto)$/i;
const COMMON_SLOGAN_REGEX = /(?:uma\s+hist[óo]ria\s+de|a\s+jornada\s+de|todos\s+os\s+direitos|leia\s+mais\s+em)/i;
function cleanCoverLine(raw) {
    return raw
        .replace(/[«»"“”„]/g, '')
        .replace(/^[^\p{L}\p{N}]+|[^\p{L}\p{N}]+$/gu, '')
        .replace(/\s+/g, ' ')
        .trim();
}
/**
 * Visually inspects a manga/manhwa cover artwork using OCR + context validation
 * to detect Brazilian Portuguese titles rendered directly onto the cover art.
 */
export async function inspectCoverForPtBrTitle(target) {
    const t0 = performance.now();
    const rawLines = [];
    const filteredCandidates = [];
    let buf = target.coverBuffer || null;
    if (!buf && target.coverUrl) {
        try {
            const res = await fetch(target.coverUrl, {
                headers: target.headers || { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
                signal: AbortSignal.timeout(15_000),
            });
            if (res.ok) {
                buf = Buffer.from(await res.arrayBuffer());
            }
        }
        catch (fetchErr) {
            return {
                inspected: false,
                foundPtBrTitle: false,
                detectedTitle: null,
                confidence: 0,
                evidence: {
                    source: target.source,
                    coverUrl: target.coverUrl,
                    rawLines: [],
                    filteredCandidates: [],
                    durationMs: Math.round(performance.now() - t0),
                    reason: `FETCH_FAILED: ${fetchErr?.message || fetchErr}`,
                },
            };
        }
    }
    if (!buf || buf.length === 0) {
        return {
            inspected: false,
            foundPtBrTitle: false,
            detectedTitle: null,
            confidence: 0,
            evidence: {
                source: target.source,
                coverUrl: target.coverUrl,
                rawLines: [],
                filteredCandidates: [],
                durationMs: Math.round(performance.now() - t0),
                reason: 'NO_IMAGE_DATA',
            },
        };
    }
    const sha256 = crypto.createHash('sha256').update(buf).digest('hex');
    const tmpImg = path.join('/tmp', `nox_vis_${sha256.slice(0, 12)}_${Date.now()}.png`);
    const tmpOut = path.join('/tmp', `nox_vis_out_${sha256.slice(0, 12)}_${Date.now()}`);
    try {
        fs.writeFileSync(tmpImg, buf);
        // Run Tesseract with custom traineddata if available, otherwise system tessdata
        const tessdataArgs = fs.existsSync(path.join(TESSDATA_DIR, 'por.traineddata'))
            ? ['--tessdata-dir', TESSDATA_DIR, '-l', 'por+eng']
            : ['-l', 'eng+spa'];
        // 1. First pass: PSM 11 (sparse text, typical for cover art titles)
        await execFileAsync('tesseract', [
            tmpImg,
            tmpOut,
            ...tessdataArgs,
            '--psm',
            '11',
        ], { timeout: 12_000 });
        if (fs.existsSync(tmpOut + '.txt')) {
            const text1 = fs.readFileSync(tmpOut + '.txt', 'utf8');
            for (const line of text1.split('\n')) {
                const cleaned = cleanCoverLine(line);
                if (cleaned.length >= 3 && !rawLines.includes(cleaned)) {
                    rawLines.push(cleaned);
                }
            }
        }
        // 2. Second pass: PSM 3 (standard automatic page segmentation) if PSM 11 produced few lines
        if (rawLines.length < 3) {
            const tmpOut2 = tmpOut + '_p3';
            try {
                await execFileAsync('tesseract', [
                    tmpImg,
                    tmpOut2,
                    ...tessdataArgs,
                    '--psm',
                    '3',
                ], { timeout: 12_000 });
                if (fs.existsSync(tmpOut2 + '.txt')) {
                    const text2 = fs.readFileSync(tmpOut2 + '.txt', 'utf8');
                    for (const line of text2.split('\n')) {
                        const cleaned = cleanCoverLine(line);
                        if (cleaned.length >= 3 && !rawLines.includes(cleaned)) {
                            rawLines.push(cleaned);
                        }
                    }
                    fs.unlinkSync(tmpOut2 + '.txt');
                }
            }
            catch {
                // Best effort second pass
            }
        }
    }
    catch (ocrErr) {
        return {
            inspected: true,
            foundPtBrTitle: false,
            detectedTitle: null,
            confidence: 0,
            evidence: {
                source: target.source,
                coverUrl: target.coverUrl,
                sha256,
                rawLines,
                filteredCandidates: [],
                durationMs: Math.round(performance.now() - t0),
                reason: `OCR_EXEC_FAILED: ${ocrErr?.message || ocrErr}`,
            },
        };
    }
    finally {
        if (fs.existsSync(tmpImg))
            fs.unlinkSync(tmpImg);
        if (fs.existsSync(tmpOut + '.txt'))
            fs.unlinkSync(tmpOut + '.txt');
    }
    // Filter out scanlator watermarks and credits
    const cleanLines = rawLines.filter((l) => !SCANLATOR_NOISE_REGEX.test(l) &&
        !CHAPTER_VOLUME_NOISE_REGEX.test(l) &&
        !COMMON_SLOGAN_REGEX.test(l));
    // Generate single-line candidates
    for (const line of cleanLines) {
        if (line.length >= 4 && line.length <= 120) {
            filteredCandidates.push(line);
        }
    }
    // Generate multi-line candidates (2-line and 3-line adjacent chunks)
    // Titles on covers frequently wrap across 2 or 3 lines of stylized text
    for (let i = 0; i < cleanLines.length - 1; i++) {
        const chunk2 = `${cleanLines[i]} ${cleanLines[i + 1]}`;
        if (chunk2.length >= 6 && chunk2.length <= 150 && !filteredCandidates.includes(chunk2)) {
            filteredCandidates.push(chunk2);
        }
        if (i < cleanLines.length - 2) {
            const chunk3 = `${cleanLines[i]} ${cleanLines[i + 1]} ${cleanLines[i + 2]}`;
            if (chunk3.length >= 8 && chunk3.length <= 180 && !filteredCandidates.includes(chunk3)) {
                filteredCandidates.push(chunk3);
            }
        }
    }
    // Rank and evaluate candidates against PT-BR rules
    const currentKey = alternativeTitleKey(target.title);
    const existingAliasKeys = new Set((target.aliases || []).map((a) => alternativeTitleKey(a)));
    const scored = [];
    for (const rawCand of filteredCandidates) {
        // Anti-hallucination: reject raw OCR candidate chunks with garbage symbols or noise
        if (/[|=%_<>{}~^\\/`*#$@]/.test(rawCand))
            continue;
        const cleaned = cleanPtBrCandidateTitle(rawCand);
        if (!cleaned || cleaned.length < 4 || cleaned.length > 150)
            continue;
        // Must be valid PT-BR
        if (!isPtBrTitle(cleaned))
            continue;
        // Must not be a franchise brand name that should stay English
        if (isConsecratedProperTitle(cleaned))
            continue;
        const words = cleaned.toLowerCase().replace(/[^\p{L}\s]/gu, ' ').split(/\s+/).filter(Boolean);
        if (words.length < 2 || words.length > 12)
            continue;
        // Anti-hallucination: check repetitive tokens (e.g. "a a", "pe pa", "e e")
        let repetitive = false;
        for (let i = 0; i < words.length - 1; i++) {
            if (words[i] === words[i + 1] && words[i].length <= 3) {
                repetitive = true;
                break;
            }
        }
        if (repetitive)
            continue;
        // Anti-hallucination: must contain at least one substantive PT word (length >= 4 in dictionary or valid PT morphology)
        const substantivePtWords = words.filter((w) => w.length >= 4 && isPortugueseWord(w));
        if (substantivePtWords.length === 0)
            continue;
        // Anti-hallucination: ratio of single-character tokens must be low (<= 25%)
        const singleCharWords = words.filter((w) => w.length <= 1);
        if (singleCharWords.length / words.length > 0.25)
            continue;
        // Anti-hallucination: ratio of valid PT words must be >= 60%
        const validPtWords = words.filter((w) => isPortugueseWord(w));
        if (validPtWords.length / words.length < 0.60)
            continue;
        const candKey = alternativeTitleKey(cleaned);
        if (!candKey || candKey === currentKey)
            continue;
        let score = 0.70; // Base confidence for valid PT-BR visual title
        // Diacritic bonus (strong evidence of Brazilian Portuguese spelling)
        if (/[áéíóúâêôãõçà]/i.test(cleaned)) {
            score += 0.15;
        }
        // Word count bonus: titles with 3-8 words are typical
        if (words.length >= 2 && words.length <= 8) {
            score += 0.10;
        }
        // If candidate was already in aliases, confidence is maximum
        if (existingAliasKeys.has(candKey)) {
            score += 0.20;
        }
        score = Math.min(0.99, score);
        scored.push({
            title: cleaned,
            confidence: score,
            rule: 'PTBR_COVER_TEXT_VALIDATED',
        });
    }
    scored.sort((a, b) => b.confidence - a.confidence || b.title.length - a.title.length);
    const best = scored[0] || null;
    const durationMs = Math.round(performance.now() - t0);
    if (best && best.confidence >= 0.70) {
        return {
            inspected: true,
            foundPtBrTitle: true,
            detectedTitle: best.title,
            confidence: best.confidence,
            method: 'PTBR_FOUND_COVER',
            evidence: {
                source: target.source,
                coverUrl: target.coverUrl,
                sha256,
                rawLines,
                filteredCandidates,
                matchedTitle: best.title,
                durationMs,
                reason: best.rule,
            },
        };
    }
    return {
        inspected: true,
        foundPtBrTitle: false,
        detectedTitle: null,
        confidence: 0,
        evidence: {
            source: target.source,
            coverUrl: target.coverUrl,
            sha256,
            rawLines,
            filteredCandidates,
            durationMs,
            reason: 'NO_PTBR_TITLE_DETECTED',
        },
    };
}
