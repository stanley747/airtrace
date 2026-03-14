import { NextResponse } from "next/server";
import { getSnapshot, SnapshotError } from "@/lib/airtrace-data";

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

  try {
    const city = await getSnapshot();

    return NextResponse.json({
      city,
      generatedAt: new Date().toISOString(),
      mode: "live-only"
    });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unknown live data failure";
    const cause =
      error instanceof SnapshotError ? error.causeLabel : "unknown";

    return NextResponse.json(
      {
        city: null,
        generatedAt: new Date().toISOString(),
        mode: "live-only",
        error: {
          cause,
          message
        }
      },
      { status: 503 }
    );
  }
}
