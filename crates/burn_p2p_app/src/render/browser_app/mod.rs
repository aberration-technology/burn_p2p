mod static_shell;
mod theme;

pub(super) fn render_browser_app_static_html(
    bootstrap: &burn_p2p_views::BrowserAppStaticBootstrap,
) -> String {
    static_shell::render_browser_app_static_html(bootstrap)
}

pub(super) fn render_browser_app_static_html_with_config(
    app_name: &str,
    asset_base_url: &str,
    module_entry_path: &str,
    stylesheet_path: Option<&str>,
    default_edge_url: Option<&str>,
    default_surface_key: &str,
    refresh_interval_ms: u64,
) -> String {
    static_shell::render_browser_app_static_html_with_config(
        app_name,
        asset_base_url,
        module_entry_path,
        stylesheet_path,
        default_edge_url,
        default_surface_key,
        refresh_interval_ms,
    )
}

pub(super) fn browser_app_stylesheet() -> &'static str {
    theme::BROWSER_PORTAL_CSS
}
