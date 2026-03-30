This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Chatbot Scaffold

The UI includes a floating LeVision chat button and a local `/api/chat` route.

To connect your own fine-tuned model or backend:

1. Copy `.env.example` to `.env.local`.
2. Set `LEVISION_CHAT_API_URL` to your chat endpoint.
3. Optionally set `LEVISION_CHAT_API_KEY` for bearer auth.
4. Optionally set `NEXT_PUBLIC_LEVISION_CHAT_LABEL` to change the label shown in the widget.

The scaffold sends this payload shape to your endpoint:

```json
{
  "messages": [
    { "role": "user", "content": "Break down our pick-and-roll coverage." }
  ],
  "app": "LeVision"
}
```

The endpoint can respond with any one of these JSON shapes:

```json
{ "message": "Assistant reply" }
```

```json
{ "content": "Assistant reply" }
```

```json
{ "reply": { "message": "Assistant reply" } }
```

If no API URL is configured, the widget stays usable with a local stub response so the frontend flow can be developed immediately.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.
