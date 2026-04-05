mod static_shell;
mod theme;

pub(super) fn render_browser_app_static_html(
    bootstrap: &burn_p2p_ui::BrowserAppStaticBootstrap,
) -> String {
    static_shell::render_browser_app_static_html(bootstrap)
}

pub(super) fn browser_app_stylesheet() -> &'static str {
    theme::BROWSER_PORTAL_CSS
}
