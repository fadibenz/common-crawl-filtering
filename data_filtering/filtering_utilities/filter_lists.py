BLACKLIST = [
    # Advertising & Promotions
    'advertisement', 'advertisements', 'advertising', 'sponsored', 'sponsor',
    'promoted content', 'promotion', 'promotions', 'buy now', 'order now',
    'shop now', 'add to cart', 'limited time', 'sale ends', 'discount code',

    # Social / Engagement CTAs
    'follow us', 'like us on', 'share this', 'share on', 'tweet', 'retweet',
    'comment below', 'leave a comment', 'join the conversation',
    'pin it', 'instagram', 'facebook', 'linkedin', 'youtube', 'subscribe',
    'subscribe to our newsletter', 'sign up for updates', 'sign up for our newsletter',
    'register now', 'create an account', 'join now', 'log in', 'sign up',

    # Navigation / UI boilerplate
    'back to top', 'scroll to top', 'skip to content', 'skip navigation',
    'next page', 'previous page', 'read more', 'click here', 'learn more',
    'view all', 'menu', 'site map',

    # Legal / Footer-like phrases
    'all rights reserved', 'terms of service', 'terms of use', 'privacy policy',
    'cookie policy', 'cookie settings', 'disclaimer', 'copyright', '©',
    '®', '™', 'legal notice', 'accessibility', 'user agreement', 'privacy statement',
    'website terms', 'website privacy', 'this site is protected by',

    # Analytics / Tracking
    'visitor counter', 'hit counter', 'tracking provided by', 'analytics', 'google analytics',
    'powered by', 'designed by', 'created with', 'hosted by', 'website design by',
    'website development by', 'theme by', 'template by',

    # File actions & Documents
    'download pdf', 'print this', 'save as pdf', 'view pdf', 'download now',

    # Contact & About
    'contact us', 'about us', 'faq', 'help center', 'support', 'customer service',
    'careers', 'job openings', 'investor relations', 'sitemap', 'newsletter archive',

    # Misc boilerplate
    'sidebar', 'navbar', 'footer', 'breadcrumbs', 'meta info', 'tag cloud',
    'related posts', 'recent posts', 'popular posts', 'archive', 'powered by wordpress',
    'built with', 'hosted on', 'switch to mobile view', 'view desktop site'
]


# terms that signal substantive, info-rich content
DOMAIN_KEYWORDS = {
    # research & reporting
    'analysis', 'report', 'study', 'research', 'survey', 'whitepaper', 'case study',
    'benchmark', 'evaluation',

    # how-to & tutorials
    'tutorial', 'guide', 'how-to', 'walkthrough', 'manual', 'documentation', 'example',
    'demo', 'demonstration', 'best practices',

    # educational & explanatory
    'overview', 'introduction', 'primer', 'deep dive', 'insights', 'explanation',
    'methodology', 'methods', 'faq', 'q&a', 'question and answer',

    # reviews & summaries
    'review', 'summary', 'recap', 'roundup', 'comparison', 'pros and cons',

    # specifications & standards
    'specification', 'standards', 'protocol', 'format', 'schema',

    # domain-specific markers
    'dataset', 'data', 'statistics', 'metrics', 'analysis pipeline',
    'framework', 'architecture', 'design patterns',
}
