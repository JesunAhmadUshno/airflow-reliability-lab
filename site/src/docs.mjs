// Build-time extraction from repo Markdown, so prose numbers are read, not retyped.
// Only the parsed fields reach the page; the raw files are not bundled.
const int = (s) => Number(s.replace(/,/g, ''));

// REPORT.md: the threats-to-validity bullets and the pre-registration commits. No grade is parsed.
export function parseReport(md) {
  const section = md.split(/^## /m).find((s) => s.startsWith('Threats to validity'));
  const threats = [...section.matchAll(/^- (.+)$/gm)].map((m) => m[1]);
  const pre = md.match(/are\s+commit (\w+) \(tag proposal-v1\); the next commits are (\w+)\s+\(research, methods, decisions\) and (\w+) \(the first experiment code\)/);
  return { threats, preregCommit: pre[1], nextCommits: [pre[2], pre[3]] };
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
      return `export default ${JSON.stringify({ report: parseReport(read('REPORT.md')), readme: parseReadme(read('README.md')), hypotheses: parseHypotheses(read('HYPOTHESES.md')) })};`;
    },
  };
}
