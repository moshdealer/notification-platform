package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/moshdealer/notification-platform/notification-service/internal/service"
	"github.com/moshdealer/notification-platform/pkg/model"
)

type NotificationHandler struct {
	service *service.NotificationService
}

func NewNotificationHandler(s *service.NotificationService) *NotificationHandler {
	return &NotificationHandler{service: s}
}

func (h *NotificationHandler) Create(c *gin.Context) {
	var req CreateNotificationRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	// маппинг DTO → model
	notification := &model.Notification{
		UserID:   req.UserID,
		Title:    req.Title,
		Body:     req.Body,
		Type:     req.Type,
		Priority: req.Priority,
		Data:     req.Data,
	}

	outboxEvent := &model.OutboxEvent{
		Topic:  "notifications.new",
		Status: "pending",
	}

	if err := h.service.Create(c.Request.Context(), notification, outboxEvent); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to create notification",
		})
		return
	}

	c.JSON(http.StatusCreated, notification)
}

func (h *NotificationHandler) MarkAsRead(c *gin.Context) {
	var req MarkAsReadRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	if err := h.service.MarkAsRead(c.Request.Context(), req.ID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to mark as read",
		})
		return
	}

	c.Status(http.StatusNoContent)
}

// TODO ошибки
