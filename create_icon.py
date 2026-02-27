#!/usr/bin/env python3
"""Generate a macOS .icns icon for Claude Retro."""

from PIL import Image, ImageDraw
import os
import shutil
import subprocess


def create_icon(size):
    """Create icon at specified size with Claude Retro branding."""
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Claude brand colors - deep purple/blue gradient
    purple = (140, 100, 255)  # Claude purple
    dark_purple = (100, 60, 200)
    accent = (100, 220, 180)  # Mint green accent

    # Draw circular background with gradient effect
    center = size // 2
    radius = int(size * 0.45)

    # Create gradient effect by drawing multiple circles
    for i in range(radius, 0, -1):
        alpha = int(255 * (i / radius))
        ratio = i / radius
        r = int(dark_purple[0] + (purple[0] - dark_purple[0]) * ratio)
        g = int(dark_purple[1] + (purple[1] - dark_purple[1]) * ratio)
        b = int(dark_purple[2] + (purple[2] - dark_purple[2]) * ratio)
        draw.ellipse(
            [center - i, center - i, center + i, center + i], fill=(r, g, b, alpha)
        )

    # Draw retrospective chart elements - ascending bars
    bar_width = max(2, size // 20)
    bar_spacing = max(2, size // 25)
    num_bars = 5
    total_width = num_bars * bar_width + (num_bars - 1) * bar_spacing
    start_x = center - total_width // 2
    base_y = center + int(size * 0.15)

    # Heights create an upward trend (improvement theme)
    heights = [0.25, 0.35, 0.45, 0.60, 0.75]

    for i, height in enumerate(heights):
        x = start_x + i * (bar_width + bar_spacing)
        bar_height = int(size * 0.3 * height)
        y = base_y - bar_height

        # Draw bar with accent color
        draw.rectangle([x, y, x + bar_width, base_y], fill=accent)

        # Add slight highlight on left edge
        if bar_width > 3:
            draw.rectangle([x, y, x + 1, base_y], fill=(150, 240, 200))

    # Draw circular arrow (retrospective/cycle symbol)
    arrow_radius = int(size * 0.35)
    arrow_width = max(2, size // 30)

    # Draw arc for circular arrow
    bbox = [
        center - arrow_radius,
        center - arrow_radius,
        center + arrow_radius,
        center + arrow_radius,
    ]
    draw.arc(bbox, start=45, end=315, fill=(255, 255, 255, 200), width=arrow_width)

    # Draw arrowhead
    arrow_size = max(4, size // 25)
    arrow_x = center + int(arrow_radius * 0.7)
    arrow_y = center - int(arrow_radius * 0.7)
    arrow_points = [
        (arrow_x, arrow_y),
        (arrow_x + arrow_size, arrow_y - arrow_size // 2),
        (arrow_x + arrow_size, arrow_y + arrow_size // 2),
    ]
    draw.polygon(arrow_points, fill=(255, 255, 255, 200))

    return img


def create_icns(output_path="icon.icns"):
    """Create macOS .icns file with all required sizes."""
    sizes = [16, 32, 64, 128, 256, 512, 1024]

    # Create iconset directory
    iconset_dir = "icon.iconset"
    if os.path.exists(iconset_dir):
        shutil.rmtree(iconset_dir)
    os.makedirs(iconset_dir)

    print("Generating icon images...")
    for size in sizes:
        img = create_icon(size)

        # Save normal resolution
        img.save(f"{iconset_dir}/icon_{size}x{size}.png")

        # Save @2x resolution (except for largest size)
        if size <= 512:
            img_2x = create_icon(size * 2)
            img_2x.save(f"{iconset_dir}/icon_{size}x{size}@2x.png")

    print("Converting to .icns...")
    # Convert to .icns using iconutil
    result = subprocess.run(
        ["iconutil", "-c", "icns", iconset_dir, "-o", output_path],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Error creating .icns: {result.stderr}")
        return False

    # Cleanup
    shutil.rmtree(iconset_dir)

    print(f"✓ Created {output_path}")
    return True


if __name__ == "__main__":
    create_icns("icon.icns")
    print("\nTo use this icon, update claude_retro.spec:")
    print("  icon='icon.icns',")
    print("\nThen rebuild with: ./build_macos.sh")
