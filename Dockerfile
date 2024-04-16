# Sử dụng image nginx làm base image
FROM nginx:alpine

# Copy file index.html vào thư mục /usr/share/nginx/html
COPY index.html /usr/share/nginx/html

# Expose cổng 80 để truy cập trang web
EXPOSE 80
