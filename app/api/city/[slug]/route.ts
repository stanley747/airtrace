import { NextResponse } from "next/server";
import { getSnapshot } from "@/lib/airtrace-data";

type RouteContext = {
  params: Promise<{
    slug: string;
  }>;
};

export async function GET(_: Request, { params }: RouteContext) {
  const { slug } = await params;

  if (slug !== "kathmandu") {
    return NextResponse.json(
      { error: "Only kathmandu is supported in this build." },
      { status: 404 }
    );
  }

  return NextResponse.json({
    city: await getSnapshot(),
    generatedAt: new Date().toISOString(),
    mode: "live-or-fallback-ai"
  });
}
