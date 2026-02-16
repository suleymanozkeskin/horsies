use catppuccin::PALETTE;
use ratatui::style::{Color, Style};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThemeFlavor {
    HorsiesDark,
    HorsiesLight,
    Latte,
    Frappe,
    Macchiato,
    Mocha,
}

impl ThemeFlavor {
    pub fn next(self) -> Self {
        match self {
            ThemeFlavor::HorsiesDark => ThemeFlavor::HorsiesLight,
            ThemeFlavor::HorsiesLight => ThemeFlavor::Latte,
            ThemeFlavor::Latte => ThemeFlavor::Frappe,
            ThemeFlavor::Frappe => ThemeFlavor::Macchiato,
            ThemeFlavor::Macchiato => ThemeFlavor::Mocha,
            ThemeFlavor::Mocha => ThemeFlavor::HorsiesDark,
        }
    }

    #[allow(dead_code)]
    pub fn label(self) -> &'static str {
        match self {
            ThemeFlavor::HorsiesDark => "Horsies Dark",
            ThemeFlavor::HorsiesLight => "Horsies Light",
            ThemeFlavor::Latte => "Latte",
            ThemeFlavor::Frappe => "Frappe",
            ThemeFlavor::Macchiato => "Macchiato",
            ThemeFlavor::Mocha => "Mocha",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Theme {
    pub flavor: ThemeFlavor,
    pub background: Color,
    pub surface: Color,
    pub surface_alt: Color,
    pub text: Color,
    pub muted: Color,
    pub accent: Color,
    pub success: Color,
    pub warning: Color,
    pub error: Color,
    pub border: Color,
}

impl Theme {
    pub fn new(flavor: ThemeFlavor) -> Self {
        match flavor {
            ThemeFlavor::HorsiesDark => Self::horsies_dark(),
            ThemeFlavor::HorsiesLight => Self::horsies_light(),
            _ => Self::from_catppuccin(flavor),
        }
    }

    fn horsies_dark() -> Self {
        Self {
            flavor: ThemeFlavor::HorsiesDark,
            background: Color::Rgb(0x05, 0x05, 0x05),   // #050505
            surface: Color::Rgb(0x0D, 0x0D, 0x0D),      // #0D0D0D
            surface_alt: Color::Rgb(0x1A, 0x1A, 0x10),   // #1A1A10
            text: Color::Rgb(0xF5, 0xF5, 0xF0),         // #F5F5F0
            muted: Color::Rgb(0x8B, 0x8B, 0x7A),        // #8B8B7A
            accent: Color::Rgb(0xDA, 0xA5, 0x20),       // #DAA520
            success: Color::Rgb(0x00, 0xFF, 0xAA),      // #00FFAA
            warning: Color::Rgb(0xFF, 0xD7, 0x00),      // #FFD700
            error: Color::Rgb(0xF4, 0x70, 0x67),        // #F47067
            border: Color::Rgb(0x5A, 0x5A, 0x4A),       // #5A5A4A
        }
    }

    fn horsies_light() -> Self {
        Self {
            flavor: ThemeFlavor::HorsiesLight,
            background: Color::Rgb(0xF7, 0xF3, 0xEB),   // #F7F3EB
            surface: Color::Rgb(0xED, 0xE8, 0xDD),      // #EDE8DD
            surface_alt: Color::Rgb(0xD6, 0xD2, 0xC6),   // #D6D2C6
            text: Color::Rgb(0x1A, 0x1A, 0x14),         // #1A1A14
            muted: Color::Rgb(0x7A, 0x7A, 0x68),        // #7A7A68
            accent: Color::Rgb(0xDA, 0xA5, 0x20),       // #DAA520
            success: Color::Rgb(0x00, 0x7A, 0x52),      // #007A52
            warning: Color::Rgb(0xC4, 0x90, 0x00),      // #C49000
            error: Color::Rgb(0xF4, 0x70, 0x67),        // #F47067
            border: Color::Rgb(0xA8, 0xA8, 0x94),       // #A8A894
        }
    }

    fn from_catppuccin(flavor: ThemeFlavor) -> Self {
        let flavor_data = match flavor {
            ThemeFlavor::Latte => PALETTE.latte,
            ThemeFlavor::Frappe => PALETTE.frappe,
            ThemeFlavor::Macchiato => PALETTE.macchiato,
            ThemeFlavor::Mocha => PALETTE.mocha,
            // Horsies variants handled in `new()`, this branch is unreachable.
            ThemeFlavor::HorsiesDark | ThemeFlavor::HorsiesLight => unreachable!(),
        };
        let colors = flavor_data.colors;
        Self {
            flavor,
            background: colors.base.into(),
            surface: colors.surface0.into(),
            surface_alt: colors.surface1.into(),
            text: colors.text.into(),
            muted: colors.subtext0.into(),
            accent: colors.mauve.into(),
            success: colors.green.into(),
            warning: colors.peach.into(),
            error: colors.red.into(),
            border: colors.overlay1.into(),
        }
    }

    pub fn next(self) -> Self {
        Self::new(self.flavor.next())
    }

    pub fn muted_style(&self) -> Style {
        Style::default().fg(self.muted)
    }

    pub fn accent_style(&self) -> Style {
        Style::default().fg(self.accent)
    }

    /// Style for toolbars (filter bars, footer hints)
    pub fn toolbar_style(&self) -> Style {
        Style::default().bg(self.surface).fg(self.text)
    }

    /// Style for toolbar borders
    pub fn toolbar_border_style(&self) -> Style {
        Style::default().fg(self.border)
    }
}
