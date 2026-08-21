import ReactMarkdown, { type Components } from 'react-markdown'
import remarkGfm from 'remark-gfm'

const components: Components = {
  h1: ({ children }) => (
    <h1 className="mt-6 text-2xl font-semibold tracking-tight text-balance first:mt-0">
      {children}
    </h1>
  ),
  h2: ({ children }) => (
    <h2 className="mt-6 text-lg font-semibold tracking-tight">{children}</h2>
  ),
  h3: ({ children }) => (
    <h3 className="mt-5 text-base font-semibold tracking-tight">{children}</h3>
  ),
  p: ({ children }) => (
    <p className="mt-4 text-[15px] leading-relaxed text-foreground/90 first:mt-0">
      {children}
    </p>
  ),
  a: ({ children, href }) => (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className="font-medium text-primary underline underline-offset-4 hover:text-primary/80"
    >
      {children}
    </a>
  ),
  ul: ({ children }) => (
    <ul className="mt-4 flex flex-col gap-1.5 pl-5 text-[15px] leading-relaxed text-foreground/90 [&>li]:list-disc">
      {children}
    </ul>
  ),
  ol: ({ children }) => (
    <ol className="mt-4 flex flex-col gap-1.5 pl-5 text-[15px] leading-relaxed text-foreground/90 [&>li]:list-decimal">
      {children}
    </ol>
  ),
  blockquote: ({ children }) => (
    <blockquote className="mt-4 border-l-2 border-primary/60 pl-4 text-[15px] italic leading-relaxed text-muted-foreground">
      {children}
    </blockquote>
  ),
  hr: () => <hr className="my-6 border-border" />,
  strong: ({ children }) => <strong className="font-semibold text-foreground">{children}</strong>,
  em: ({ children }) => <em className="italic">{children}</em>,
  code: ({ children }) => (
    <code className="rounded bg-muted px-1.5 py-0.5 font-mono text-[13px] text-foreground/90">
      {children}
    </code>
  ),
}

// The mock crawler embeds the headline as a leading "# Title" line in the
// body markdown. The detail views already render their own <h1> for the
// title, so strip that first line here to avoid showing it twice.
function stripLeadingTitle(content: string) {
  return content.replace(/^#\s+.+\n+/, '')
}

export function MarkdownContent({ content }: { content: string }) {
  return (
    <div>
      <ReactMarkdown skipHtml remarkPlugins={[remarkGfm]} components={components}>
        {stripLeadingTitle(content)}
      </ReactMarkdown>
    </div>
  )
}
