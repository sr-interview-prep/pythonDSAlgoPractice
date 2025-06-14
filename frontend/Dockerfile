# STAGE 1: Build the React application
FROM node:18-alpine AS build-stage

WORKDIR /app

# Copy package.json and package-lock.json (or yarn.lock if used)
# package-lock.json is crucial for reproducible builds with npm.
# If it's not present in the source, npm install will generate it or use package.json alone.
# For this project, we manually created package.json, so package-lock.json might not exist yet.
# Docker will gracefully handle it if package-lock.json is not found by the COPY command.
COPY package.json ./
# COPY package-lock.json ./
# Removed copying package-lock.json as it might not exist in this scripted setup.
# In a real dev workflow, package-lock.json would be committed and copied.
# npm install will work with just package.json.

# Install dependencies
RUN npm install

# Copy the rest of the application source code
COPY . .

# Build the application
RUN npm run build
# The output is typically in /app/dist or /app/build for Vite/CRA
# Vite (used in this project) outputs to a 'dist' directory.

# STAGE 2: Serve the application with Nginx
FROM nginx:alpine

# Copy the Nginx configuration file
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy the built static assets from the build stage
# Vite typically builds to 'dist'.
COPY --from=build-stage /app/dist /usr/share/nginx/html

# Expose port 80 (Nginx default)
EXPOSE 80

# Command to run Nginx in the foreground
CMD ["nginx", "-g", "daemon off;"]
