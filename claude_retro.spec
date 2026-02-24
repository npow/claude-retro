# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for Claude Retro macOS .app bundle."""

from PyInstaller.utils.hooks import collect_dynamic_libs

block_cipher = None

duckdb_binaries = collect_dynamic_libs('duckdb')

a = Analysis(
    ['launcher.py'],
    pathex=[],
    binaries=duckdb_binaries,
    datas=[('claude_retro/static', 'static')],
    hiddenimports=[
        'claude_retro',
        'claude_retro.config',
        'claude_retro.db',
        'claude_retro.server',
        'claude_retro.ingest',
        'claude_retro.sessions',
        'claude_retro.features',
        'claude_retro.scoring',
        'claude_retro.intents',
        'claude_retro.baselines',
        'claude_retro.prescriptions',
        'claude_retro.llm_judge',
        'claude_retro.background',
        'claude_retro.digest',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='Claude Retro',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
    disable_windowed_traceback=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    name='Claude Retro',
)

app = BUNDLE(
    coll,
    name='Claude Retro.app',
    icon='icon.icns',
    bundle_identifier='com.claude-retro.app',
    info_plist={
        'CFBundleShortVersionString': '0.1.0',
        'NSHighResolutionCapable': True,
    },
)
