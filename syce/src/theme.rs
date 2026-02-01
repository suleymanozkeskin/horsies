use catppuccin::PALETTE;
use ratatui::style::{Color, Style};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThemeFlavor {
    Latte,
    Frappe,
    Macchiato,
    Mocha,
}

impl ThemeFlavor {
    pub fn next(self) -> Self {
        match self {
            ThemeFlavor::Latte => ThemeFlavor::Frappe,
            ThemeFlavor::Frappe => ThemeFlavor::Macchiato,
            ThemeFlavor::Macchiato => ThemeFlavor::Mocha,
            ThemeFlavor::Mocha => ThemeFlavor::Latte,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
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
        let flavor_data = match flavor {
            ThemeFlavor::Latte => PALETTE.latte,
            ThemeFlavor::Frappe => PALETTE.frappe,
            ThemeFlavor::Macchiato => PALETTE.macchiato,
            ThemeFlavor::Mocha => PALETTE.mocha,
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

    pub fn surface_style(&self) -> Style {
        Style::default().bg(self.surface).fg(self.text)
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
