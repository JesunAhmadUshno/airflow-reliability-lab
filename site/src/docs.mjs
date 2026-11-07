// Build-time extraction from repo Markdown, so prose numbers are read, not retyped.
// Only the parsed fields reach the page; the raw files are not bundled.
const int = (s) => Number(s.replace(/,/g, ''));

export function parseAudit(md) {
  const [, grade, scale] = md.match(/## Grade: ([\d,]+) of ([\d,]+)/);
  const deductions = [...md.matchAll(/^\| (-[\d,]+) \| (.+?) \|$/gm)].map(([, p, item]) => ({ points: int(p), item }));
  const total = int(md.match(/\| Total (-[\d,]+) \|/)[1]);
  const pre = md.match(/are\s+commit (\w+); the first experiment code is (\w+) and (\w+)/);
  return { grade: int(grade), scale: int(scale), deductions, total, preregCommit: pre[1], codeCommits: [pre[2], pre[3]] };
}

export function parseReadme(md) {
  const [, minutes, cores] = md.match(/about\s+(\d+)\s+minutes on (\d+) cores/);
  return { runtimeMinutes: int(minutes), cores: int(cores) };
}

// HYPOTHESES.md: one entry per "## Hn (RQn): title" section.
export function parseHypotheses(md) {
  return md.split(/^## /m).slice(1).filter((s) => /^H\d/.test(s)).map((s) => {
    const [head, ...body] = s.trim().split('\n');
    const [, id, rq, title] = head.match(/^(H\d) \((RQ\d)\): (.+)$/);
    const text = body.join('\n').trim().replace(/\n(?![-\n])/g, ' ');
    return { id, rq, title, text };
  });
}

export function repoDocsPlugin(read) {
  const id = 'virtual:repo-docs';
  return {
    name: 'repo-docs',
    resolveId: (s) => (s === id ? '\0' + id : null),
    load(s) {
      if (s !== '\0' + id) return null;
      return `export default ${JSON.stringify({ audit: parseAudit(read('AUDIT.md')), readme: parseReadme(read('README.md')), hypotheses: parseHypotheses(read('HYPOTHESES.md')) })};`;
    },
  };
}
