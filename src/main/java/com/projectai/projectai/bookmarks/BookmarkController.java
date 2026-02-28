package com.projectai.projectai.bookmarks;

import com.projectai.projectai.auth.AuthModels;
import com.projectai.projectai.auth.AuthService;
import com.projectai.projectai.auth.CsrfService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/bookmarks")
public class BookmarkController {

    private final BookmarkService bookmarkService;
    private final AuthService authService;
    private final CsrfService csrfService;

    public BookmarkController(BookmarkService bookmarkService, AuthService authService, CsrfService csrfService) {
        this.bookmarkService = bookmarkService;
        this.authService = authService;
        this.csrfService = csrfService;
    }

    @GetMapping("/groups")
    public ResponseEntity<List<BookmarkModels.BookmarkGroupResponse>> listGroups(HttpSession session) {
        AuthModels.AuthUserResponse user = authService.requireCurrentUser(session);
        return ResponseEntity.ok(bookmarkService.listGroups(user.userId()));
    }

    @PostMapping("/groups")
    public ResponseEntity<BookmarkModels.BookmarkGroupResponse> createGroup(
            @RequestBody BookmarkModels.CreateGroupRequest request,
            HttpServletRequest httpRequest,
            HttpSession session
    ) {
        csrfService.requireValidToken(httpRequest, session);
        AuthModels.AuthUserResponse user = authService.requireCurrentUser(session);
        return ResponseEntity.ok(bookmarkService.createGroup(user.userId(), request));
    }

    @PatchMapping("/groups/{groupId}")
    public ResponseEntity<BookmarkModels.BookmarkGroupResponse> renameGroup(
            @PathVariable long groupId,
            @RequestBody BookmarkModels.RenameGroupRequest request,
            HttpServletRequest httpRequest,
            HttpSession session
    ) {
        csrfService.requireValidToken(httpRequest, session);
        AuthModels.AuthUserResponse user = authService.requireCurrentUser(session);
        return ResponseEntity.ok(bookmarkService.renameGroup(user.userId(), groupId, request));
    }

    @DeleteMapping("/groups/{groupId}")
    public ResponseEntity<Void> deleteGroup(
            @PathVariable long groupId,
            HttpServletRequest httpRequest,
            HttpSession session
    ) {
        csrfService.requireValidToken(httpRequest, session);
        AuthModels.AuthUserResponse user = authService.requireCurrentUser(session);
        bookmarkService.deleteGroup(user.userId(), groupId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/items")
    public ResponseEntity<List<BookmarkModels.BookmarkItemResponse>> listItems(
            @RequestParam(required = false) Long groupId,
            HttpSession session
    ) {
        AuthModels.AuthUserResponse user = authService.requireCurrentUser(session);
        return ResponseEntity.ok(bookmarkService.listItems(user.userId(), groupId));
    }

    @PostMapping("/items")
    public ResponseEntity<BookmarkModels.BookmarkItemResponse> addItem(
            @RequestBody BookmarkModels.AddBookmarkRequest request,
            HttpServletRequest httpRequest,
            HttpSession session
    ) {
        csrfService.requireValidToken(httpRequest, session);
        AuthModels.AuthUserResponse user = authService.requireCurrentUser(session);
        return ResponseEntity.ok(bookmarkService.addItem(user.userId(), request));
    }

    @PatchMapping("/items/{bookmarkId}/group")
    public ResponseEntity<BookmarkModels.BookmarkItemResponse> moveItemToGroup(
            @PathVariable long bookmarkId,
            @RequestBody BookmarkModels.MoveBookmarkRequest request,
            HttpServletRequest httpRequest,
            HttpSession session
    ) {
        csrfService.requireValidToken(httpRequest, session);
        AuthModels.AuthUserResponse user = authService.requireCurrentUser(session);
        return ResponseEntity.ok(bookmarkService.moveItemToGroup(user.userId(), bookmarkId, request.groupId()));
    }

    @DeleteMapping("/items/{bookmarkId}")
    public ResponseEntity<Void> deleteItem(
            @PathVariable long bookmarkId,
            HttpServletRequest httpRequest,
            HttpSession session
    ) {
        csrfService.requireValidToken(httpRequest, session);
        AuthModels.AuthUserResponse user = authService.requireCurrentUser(session);
        bookmarkService.deleteItem(user.userId(), bookmarkId);
        return ResponseEntity.noContent().build();
    }
}
