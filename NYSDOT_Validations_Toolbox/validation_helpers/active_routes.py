ACTIVE_ROUTES_QUERY = (
    '(FROM_DATE IS NULL OR FROM_DATE <= CURRENT_TIMESTAMP) AND (TO_DATE IS NULL OR TO_DATE >= CURRENT_TIMESTAMP)'
)