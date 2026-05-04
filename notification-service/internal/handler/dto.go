package handler

type CreateNotificationRequest struct {
	UserID   string         `json:"user_id" binding:"required"`
	Title    string         `json:"title" binding:"required"`
	Body     string         `json:"body"`
	Type     string         `json:"type"`
	Priority string         `json:"priority"`
	Data     map[string]any `json:"data"`
}

type MarkAsReadRequest struct {
	ID uint `json:"id" binding:"required"`
}
