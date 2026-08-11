// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

const site = "https://ardiq.bytay.dev";
const description =
  "A featherweight task queue for Python with a Rust core: high throughput on a tiny memory footprint, backed by Redis Streams. Priorities, cron, retries, aborts and typed enqueue included.";

export default defineConfig({
  site,
  integrations: [
    starlight({
      title: "ArdiQ",
      description,
      customCss: ["./src/styles/custom.css"],
      components: {
        PageTitle: "./src/components/PageTitle.astro",
        Footer: "./src/components/Footer.astro",
      },
      head: [
        {
          tag: "meta",
          attrs: { property: "og:image", content: `${site}/og.png` },
        },
        { tag: "meta", attrs: { property: "og:image:width", content: "1200" } },
        { tag: "meta", attrs: { property: "og:image:height", content: "630" } },
        {
          tag: "meta",
          attrs: { name: "twitter:image", content: `${site}/og.png` },
        },
        {
          tag: "meta",
          attrs: { name: "twitter:card", content: "summary_large_image" },
        },
        {
          tag: "script",
          attrs: {
            defer: true,
            src: "https://static.cloudflareinsights.com/beacon.min.js",
            "data-cf-beacon": JSON.stringify({
              token: "df19b5b497cf46d898a4d2e09454f4e8",
            }),
          },
        },
        {
          tag: "script",
          attrs: { type: "application/ld+json" },
          content: JSON.stringify({
            "@context": "https://schema.org",
            "@type": "SoftwareApplication",
            name: "ArdiQ",
            applicationCategory: "DeveloperApplication",
            operatingSystem: "Linux",
            description,
            url: site,
            softwareVersion: "0.5.0",
            programmingLanguage: ["Python", "Rust"],
            license: "https://opensource.org/licenses/MIT",
            author: { "@type": "Person", name: "17tayyy" },
            offers: { "@type": "Offer", price: "0", priceCurrency: "USD" },
            sameAs: [
              "https://github.com/17tayyy/ardiq",
              "https://pypi.org/project/ardiq/",
            ],
          }),
        },
      ],
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/17tayyy/ardiq",
        },
      ],
      editLink: { baseUrl: "https://github.com/17tayyy/ardiq/edit/main/docs/" },
      sidebar: [
        {
          label: "Start here",
          items: [
            { label: "Introduction", slug: "guides/introduction" },
            { label: "Getting started", slug: "guides/getting-started" },
            { label: "Performance", slug: "guides/performance" },
          ],
        },
        {
          label: "Guides",
          items: [
            { label: "Defining tasks", slug: "guides/tasks" },
            { label: "Enqueuing & scheduling", slug: "guides/enqueuing" },
            { label: "Recurring tasks", slug: "guides/recurring" },
            { label: "Results & introspection", slug: "guides/results" },
            { label: "Handling failures", slug: "guides/errors" },
            { label: "Aborting tasks", slug: "guides/aborting" },
            { label: "Shared resources", slug: "guides/lifespan" },
            { label: "Running a worker", slug: "guides/worker" },
            { label: "Serialization", slug: "guides/serialization" },
          ],
        },
        {
          label: "Reference",
          items: [
            { label: "Configuration", slug: "reference/configuration" },
            { label: "Python API", slug: "reference/api" },
            { label: "CLI", slug: "reference/cli" },
          ],
        },
      ],
    }),
  ],
});
