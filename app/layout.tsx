import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "AirTrace",
  description:
    "Air pollution source attribution for cities affected by transported PM2.5."
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
