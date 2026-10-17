# Frontend SPA servido por nginx (estático).
FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html config.js app.js /usr/share/nginx/html/
EXPOSE 80
